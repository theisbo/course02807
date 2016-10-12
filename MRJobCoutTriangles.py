from mrjob.job import MRJob
import sys

class MRJobCountTriangles(MRJob):
	def steps(self):
		return[
			self.mr(
				mapper=self.map_connections,
				reducer=self.reduce_pairs
			),
			self.mr(
				mapper=self.group_pairs,
				reducer=self.find_intersections
			)
			,
			self.mr(
				mapper=self.count_triangles,
				reducer=self.sum_triangles
			)
		]
	def map_connections(self, _, edge):
		points = edge.split()
#		sys.stderr.write("MAPPER INPUT: ({0},{1})\n".format(None,edge))
		yield (int(points[0]), int(points[1]))
		yield (int(points[1]), int(points[0]))
	def reduce_pairs(self, points, connections):
#		sys.stderr.write("REDUCER INPUT: ({0},{1})\n".format(points,connections))
		values = list(connections)
		for cp in values:
			if points < cp:
				yield ([points, cp], values)
			else:
				yield ([cp, points], values)
	def group_pairs(self, pairs, connections):
#		sys.stderr.write("MAPPER INPUT: ({0},{1})\n".format(pairs,connections))
		yield (pairs, connections)
	def find_intersections(self, pairs, connections):
#		sys.stderr.write("MAPPER INPUT: ({0},{1})\n".format(pairs,connections))
		values = list(connections)
		if len(values) > 1:
			for p_idx in range(len(values[0])):
				point = values[0][p_idx]
				if point in values[1]:
					yield (pairs, point)
	def count_triangles(self, pairs, points):
		yield ("triangle", 1)
	def sum_triangles(self, triangles, count):
		yield ("Total triangles", sum(count)/3)

if __name__ == '__main__':
	MRJobCountTriangles.run()
