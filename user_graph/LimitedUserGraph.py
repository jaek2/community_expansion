import pydoop.hdfs as hdfs
from igraph import *
import cairocffi as cairo
import sys
import heapq
import numpy as np
import datetime

# the key is we are using only part of the feature weight vector from Vowpal Wabbit
# that way the similarity calculation and input data processing become less expensive
# while ensuring the graph to be sparse, which is required by community detection algorithms
class UserGraph(object):

    # keep the list of SIDs for the original users
    # retain the featureList to be used for the similarity function
    # cutoff is used to determine wherether or not to make an edge between two users
    # also maintain igraph graph, which better be in adjacency list form
    def __init__(self, pathForOriginalUsers, pathForFeature, pathForLookalikeInput, cutoff=0.8):
        self.g = None
        self.original_clustering = None

        self.num_user = 0
        self.num_vertex = 0
        self.num_edge = 0
        self.num_community = 0

        self.vertices = None
        self.vectex_mapping = {}
        self.a_dict = {}    # dictrionary for a_i

        self.pathForOriginalUsers = pathForOriginalUsers
        self.pathForFeature = pathForFeature
        self.pathForLookalikeInput = pathForLookalikeInput
        self.featureVector = None
        self.cutoff = cutoff

    # read graph input data and build a map {tuple : count}
    def read_input_data(self):
        self.arr = []

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Start reading input data")

        for hdfs_file in hdfs.fs.hdfs().list_directory(self.pathForOriginalUsers):
            with hdfs.open(hdfs_file['path']) as f:
                for line in f:

            #with open(pathForData) as f:
            #    for line in f:
                    self.num_user += 1

                    sid = ''
                    demo = set([])
                    device = set([])
                    app = set([])
                    geo = set([])
                    keywords = set([])

                    # split the line for different namespace
                    temp_line = line.split('|')

                    for temp in temp_line:
                        temp_arr = temp.strip().split()

                        # put each data into correct namespace and project them through the feature vector
                        namespace = temp_arr[0]
                        if namespace == 'demo':
                            demo = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[0]))))
                        elif namespace == 'device':
                            device = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[1]))))
                        elif namespace == 'app_activities':
                            app = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[2]))))
                        elif namespace == 'geo':
                            geo = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[3]))))
                        elif namespace == 'keywords':
                            keywords = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[4]))))
                        else:
                            sid = temp_arr[0]

                    if len(keywords) != 0 or len(device) != 0 or len(geo) != 0 or len(demo) != 0 or len(app) != 0:
                        temp_vertex_feature_tuple = (tuple(demo), tuple(device), tuple(app), tuple(geo), tuple(keywords))

                        if temp_vertex_feature_tuple in self.vectex_mapping:
                            self.vectex_mapping[temp_vertex_feature_tuple] += 1
                        else:
                            # weight of the self loop
                            self.vectex_mapping[temp_vertex_feature_tuple] = 1

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Just read data from %d users" % self.num_user)

        # save the projected data
        #self.arr = arr # tuple (string, set of strings, set of strings, set of strings, set of strings, set of strings)

    # read data of originally targetted users and construct the graph
    def constructGraph(self):
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Feature Vector "),
        print(self.featureVector)

        # read in the input path
        self.read_input_data()

        # add vertices
        self.vertices = self.vectex_mapping.keys()
        self.num_vertex = len(self.vertices)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Adding vetices n = %d" % self.num_vertex)

        self.g = Graph(self.num_vertex)

        edge_list = []
        edge_weight_list = []
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Adding edges")
        cnt = 0
        # add edges
        # these are normal edges with weight 1
        for i in range(self.num_vertex):
            for j in range(i+1, self.num_vertex):
                if self.similarity(self.vertices[i], self.vertices[j]) >= self.cutoff:
                    edge_list.append((i,j))
                    edge_weight_list.append(self.vectex_mapping[self.vertices[i]] * self.vectex_mapping[self.vertices[j]])

                    cnt += 1

        for i in range(self.num_vertex):
            # make a self loop with weight k(k-1)/2
            edge_list.append((i,i))

            k = self.vectex_mapping[self.vertices[i]]
            edge_weight_list.append(k*(k-1)/2)

        cnt += self.num_vertex
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Total number of edges = %d" % cnt)

        # add them
        self.g.add_edges(edge_list)
        self.num_edge = len(edge_list)
        self.g.es["weight"] = edge_weight_list

    # read info vector for two users calculate the similarity based on the featureList
    # this function uses Jaccard Index
    # but first, I'll set the similarity to zero if both are empty
    def similarity(self, user_a, user_b):
        length_a = 0
        length_b = 0
        length_i = 0

        for i in range (0, 5):
            length_a += len(user_a[i])
            length_b += len(user_b[i])
            length_i += len(set(user_a[i]).intersection(set(user_b[i])))

        # original Jaccard Index assigns 1 for this case
        if length_a == 0 and length_b == 0:
            return 0.0

        # in normal case, calculate index and return
        return 1.0 * length_i / (length_a + length_b - length_i)

    # try different methods of community detection and find the best one
    def findCommunity(self):
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community detection")

        self.original_clustering = self.g.community_fastgreedy(weights="weight").as_clustering()

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print('Modularity = %f' % self.original_clustering.modularity)

        # calculate a_i value for each community
        degree = self.g.degree()

        # accumulate the number of degree of the vertices that belong to the community
        for i in range (self.num_vertex):
            if self.original_clustering.membership[i] in self.a_dict:
                self.a_dict[self.original_clustering.membership[i]] += degree[i]
            else:
                self.a_dict[self.original_clustering.membership[i]] = 1.0 * degree[i]

        # divide by two time the number of edges
        for key in self.a_dict:
            self.num_community += 1
            self.a_dict[key] /= 2 * self.num_edge

    def parameterTuning(self):
        if self.num_edge == 0:
            print("["),
            print(datetime.datetime.utcnow()),
            print("]"),
            print("No edge, terminating the run")
            return

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community fastgreedy... "),
        clustering = self.g.community_fastgreedy(weights="weight").as_clustering()
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        if clustering.modularity is None:
            print('Modularity = None')
        else:
            print('Modularity = %f' % clustering.modularity)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community infomap... "),
        clustering = self.g.community_infomap(weights="weight")
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        if clustering.modularity is None:
            print('Modularity = None')
        else:
            print('Modularity = %f' % clustering.modularity)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community leading eigenvector... "),
        clustering = self.g.community_leading_eigenvector(weights="weight")
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        if clustering.modularity is None:
            print('Modularity = None')
        else:
            print('Modularity = %f' % clustering.modularity)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community label propagation... "),
        clustering = self.g.community_label_propagation(weights="weight")
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        if clustering.modularity is None:
            print('Modularity = None')
        else:
            print('Modularity = %f' % clustering.modularity)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting community walktrap... "),
        clustering = self.g.community_walktrap(weights="weight").as_clustering()
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        if clustering.modularity is None:
            print('Modularity = None')
        else:
            print('Modularity = %f' % clustering.modularity)

    # read data of users from the lookalike expansion,
    # add users to the graph one by one and keep them only if they increase the modularity
    # return the sid list of the users at the end (exclude the original users)
    def incrementUsers(self):
        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Starting incrementing")

        # declare expanded user list to be returned
        expanded_users = []
        fs_dict = {} # featrue_vector : sid list

        cnt = 0
        # everything will be done with reading the expanded user data line by line
        # note that we don't really need to add the vertex to the graph
        # just need to get the number of edges to each community

        # fisrt reduce the size using map
        for hdfs_file in hdfs.fs.hdfs().list_directory(self.pathForLookalikeInput):
            with hdfs.open(hdfs_file['path']) as f:
            #with open(self.pathForLookalikeInput) as f:
                for line in f:
                    sid = ''
                    demo = []
                    device = []
                    app = []
                    geo = []
                    keywords = []

                    # split the line for different namespace
                    temp_line = line.split('|')

                    for temp in temp_line:
                        temp_arr = temp.strip().split()

                        # put each data into correct namespace and project them through the feature vector
                        namespace = temp_arr[0]
                        if namespace == 'demo':
                            demo = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[0]))))
                        elif namespace == 'device':
                            device = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[1]))))
                        elif namespace == 'app_activities':
                            app = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[2]))))
                        elif namespace == 'geo':
                            geo = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[3]))))
                        elif namespace == 'keywords':
                            keywords = sorted(list(set(temp_arr[1:]).intersection(set(self.featureVector[4]))))
                        else:
                            sid = temp_arr[0]

                    if len(keywords) != 0 or len(device) != 0 or len(geo) != 0 or len(demo) != 0 or len(app) != 0:
                        temp_vertex_feature_tuple = (tuple(demo), tuple(device), tuple(app), tuple(geo), tuple(keywords))
                        if temp_vertex_feature_tuple in fs_dict:
                            fs_dict[temp_vertex_feature_tuple].append(sid)
                        else:
                            fs_dict[temp_vertex_feature_tuple] = [sid]

                    cnt += 1
                    if cnt % 1000000 == 0:
                        print("["),
                        print(datetime.datetime.utcnow()),
                        print("]"),
                        print("Processed %d possible users" % cnt)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Processed total %d possible users" % cnt)
        num_lookalike = cnt

        # process the map
        print("There are %d tuples" % len(fs_dict))
        cnt = 0
        for vertex_feature_tuple in fs_dict:
            # declare necessary variables
            community_num_edge_list = np.zeros(self.num_community)
            num_total_new_edge = 0
            need_to_check = False

            # iterate through all the vertices in the original graph
            for i in range(self.num_vertex):
                # check if we should make an edge between the new vertex and the vertex in the original graph
                if self.similarity(vertex_feature_tuple, self.vertices[i]) >= self.cutoff:
                    need_to_check = True

                    # increment total number
                    num_total_new_edge += self.vectex_mapping[self.vertices[i]]

                    # save it to the community : num_edge map
                    community_num_edge_list[self.original_clustering.membership[i]] += self.vectex_mapping[self.vertices[i]]

            if need_to_check:
                assigned_community_idx = np.argmax(community_num_edge_list)

                if community_num_edge_list[assigned_community_idx] > self.a_dict[assigned_community_idx] * num_total_new_edge:
                    expanded_users.extend([(sid, community_num_edge_list[assigned_community_idx]-self.a_dict[assigned_community_idx] * num_total_new_edge) for sid in fs_dict[vertex_feature_tuple]])

            cnt += 1
            if cnt % 1000 == 0:
                print("["),
                print(datetime.datetime.utcnow()),
                print("]"),
                print("Examined %d possible tuples" % cnt)

        print("["),
        print(datetime.datetime.utcnow()),
        print("]"),
        print("Examined total %d possible tuples" % cnt)

        # return the result
        top_users = heapq.nlargest(min(num_lookalike/1000, len(expanded_users)), expanded_users, key = lambda x : x[1])
        #top_users = heapq.nlargest(self.num_user, expanded_users, key = lambda x : x[1])
        return [user_tuple[0] for user_tuple in top_users]

    # plot the graph
    def plot(self):
        lo = self.g.layout_reingold_tilford_circular()
        plot(self.g, 'plot.png', layout = lo)

    # path for the feature file and size of the feature vector to be returned
    def get_feature_vector(self, num_feature):
        # open file
        with open(self.pathForFeature) as f:
            lines = f.readlines();

        demo = []
        device = []
        geo = []
        app = []
        keywords = []
        names = ['demo', 'device', 'geo', 'app_activities', 'keywords']

        # find the starting point
        start = 0
        flag = True
        while flag:
            if len(lines[start].split('^')) > 1:
                flag = False
            start += 1

        arr = []
        # fill in the data
        for line in lines[start:]:
            temp = line.split(':')
            string_part = temp[0]
            weight = abs(float(temp[2]))

            arr.append((string_part, weight))

        # select the top k variables
        feature_vector = heapq.nlargest(num_feature, arr, key = lambda x : x[1])

        # process them
        for feature in feature_vector:
            temp = feature[0].split('^')
            namespace = temp[0]
            variable = temp[1]

            if namespace == names[0]:
                demo.append(variable)
            elif namespace == names[1]:
                device.append(variable)
            elif namespace == names[2]:
                geo.append(variable)
            elif namespace == names[3]:
                app.append(variable)
            else:
                keywords.append(variable)

        self.featureVector = (demo, device, geo, app, keywords)

##############################################################################################

# read in argument and create a UserGraph object properly
if __name__ == "__main__":
    # ex) ../Python2.7.10/bin/python FUserGraph.py 20702705
    graph_input_path_base = '/tmp/jaek/graph_input/'
    feature_path_base = '/homes/adwstg/jaek/pipeline_dev/feature_vector/'
    feature_path_suffix = '.feature'

    segment_id = sys.argv[1] # 20702705, 20033178, other segments
    expanded_date = sys.argv[2] # 20170710, 20170715, others have 20170721
    num_feature = int(sys.argv[3]) # 10

    graph_input_path = graph_input_path_base + segment_id

    feature_path = feature_path_base + segment_id + feature_path_suffix

    lookalike_graph_input_path_base = '/tmp/jaek/lookalike_graph_input/'
    lookalike_graph_input_path = lookalike_graph_input_path_base + segment_id + '/' + expanded_date

    graph = UserGraph(graph_input_path, feature_path, lookalike_graph_input_path)
    graph.get_feature_vector(num_feature)
    graph.constructGraph()
    graph.findCommunity()

    output = graph.incrementUsers()

    print("["),
    print(datetime.datetime.utcnow()),
    print("]"),
    print("Now printing %d sid list" % len(output))
    #output_path = '/grid/0/tmp/jaek/final_output/' + segment_id + '_'  + str(num_feature) + 'f_expanded_sid_list'
    output_path = '/tmp/jaek/final_output/' + segment_id + '_'  + str(num_feature) + 'f_limited_expanded_sid_list'
    output_file = hdfs.open(output_path, 'w')
    for item in output:
        output_file.write("%s\n" % item)
    output_file.close()

# I need a way to somehow determine which community a new user has to be assisned
# Running community detection again with the incremented graph is too expensive.
# I need to cut it down to a constant time or a time proportional to the number of the communities
