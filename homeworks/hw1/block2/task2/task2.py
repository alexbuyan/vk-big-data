
from mrjob.job import MRJob, MRStep

class Task2(MRJob):
    def mapper(self, _, line):
        words = line.lower().replace('"', '').split()[1:]
        person = words[0]
        phrase = ' '.join(words[1:])
        yield (person, (phrase, len(phrase)))

    def reducer(self, person, phrase_and_len):
        longest_phrase, phrase_len = max(phrase_and_len, key=lambda l_p: l_p[1])
        yield None, (person, longest_phrase, phrase_len)

    def top_person_phrase(self, _, person_phrase_len):
        person_phrase_len = list(person_phrase_len)
        person_phrase_len.sort(key=lambda cc: cc[2], reverse=True)
        for person, phrase, phrase_len in person_phrase_len:
            yield person, phrase

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.top_person_phrase)
        ]

if __name__ == "__main__":
    Task2.run()
