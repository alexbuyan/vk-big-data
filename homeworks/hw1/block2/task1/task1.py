
from mrjob.job import MRJob, MRStep

class Task1(MRJob):
    def mapper(self, _, line):
        words = line.lower().replace('"', '').split()[1:]
        person = words[0]
        yield (person, 1)

    def combiner(self, character, cnts):
        yield (character, sum(cnts))

    def reducer(self, character, cnts):
        yield None, (character, sum(cnts))

    def top_20_characters(self, _, character_count):
        character_count = list(character_count)
        character_count.sort(key=lambda cc: cc[1], reverse=True)
        for character, count in character_count[:20]:
            yield (character, count)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.top_20_characters)
        ]

if __name__ == "__main__":
    Task1.run()
