
from mrjob.job import MRJob, MRStep

class Task1(MRJob):
    def mapper(self, _, line):
        words = line.lower().replace('"', '').split()[1:]
        person = words[0]
        yield (person, 1)

    def combiner(self, person, person_counts):
        yield (person, sum(person_counts))

    def reducer(self, person, person_counts):
        yield None, (person, sum(person_counts))

    def top_20_persons(self, _, person_and_count):
        person_and_count = list(person_and_count)
        person_and_count.sort(key=lambda p_c: p_c[1], reverse=True)
        for person, count in person_and_count[:20]:
            yield (person, count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.top_20_persons)
        ]

if __name__ == "__main__":
    Task1.run()
