from mrjob.job import MRJob

class MRJobWordOccurence(MRJob):
	def steps(self):
		return [
		self.mr(mapper=self.map_to_lower),
		self.mr(mapper=self.map_remove_special,
			combiner=self.comb_count_in_lines,
			reducer=self.redu_count_in_doc)
		]
	def map_to_lower(self, _, line):
		for word in line.split():
			yield (word.lower(),1)
	def map_remove_special(self, word, count):
		cleanword = ''.join(c for c in word if c.isalnum())
		yield (cleanword,count)
	def comb_count_in_lines(self, word, count):
		yield (word,sum(count))
	def redu_count_in_doc(self, word, count):
		yield (word,sum(count))

if __name__ == '__main__':
	MRJobWordOccurence.run()
