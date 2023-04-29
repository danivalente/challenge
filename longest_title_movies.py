from mrjob.job import MRJob
import csv

class LongestTitleMovies(MRJob):

    def mapper(self, _, line):
        user_id, movie_id, rating, timestamp = line.strip().split('\t')
        yield movie_id, (len(movie_id), float(rating))

    def combiner(self, movie_id, values):
        total_len = 0
        total_rating = 0
        count = 0
        for len_rating in values:
            total_len += len_rating[0]
            total_rating += len_rating[1]
            count += 1
        yield movie_id, (total_len, total_rating, count)

    def reducer(self, movie_id, values):
        total_len = 0
        total_rating = 0
        count = 0
        for len_rating_count in values:
            total_len += len_rating_count[0]
            total_rating += len_rating_count[1]
            count += len_rating_count[2]
        if count >= 10:
            with open('/path/to/u.item', 'r', encoding='ISO-8859-1') as f:
                reader = csv.reader(f, delimiter='|')
                for row in reader:
                    if row[0] == movie_id:
                        title = row[1]
                        break
            yield None, (title, total_len/count)

    def reducer_top10(self, _, title_len_pairs):
        top10 = sorted(title_len_pairs, key=lambda x: x[1], reverse=True)[:10]
        for title, length in top10:
            yield title, length

    def steps(self):
        return [
            self.mr(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            self.mr(reducer=self.reducer_top10)
        ]

if __name__ == '__main__':
    LongestTitleMovies.run()
