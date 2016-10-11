from mrjob.job import MRJob

class MRJobEulerGraph(MRJob):
	def steps(self):
		return [
		self.mr(mapper=self.map_split_data,
			combiner=self.comb_count_connections,
			reducer=self.redu_count_connections)
		]
	def map_split_data(self, _, edge):
		for vertex in edge.split():
			yield (vertex, 1)
	def comb_count_connections(self, vertex, connections):
		yield (vertex, sum(connections))
	def redu_count_connections(self, vertex, connections):
		yield (vertex, sum(connections))
	
if __name__ == '__main__':
	MRJobEulerGraph.run()
