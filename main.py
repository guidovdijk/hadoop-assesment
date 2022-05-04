from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):     
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_rating_count, 
                    combiner=self.combine_count_ratings,
                    reducer=self.reducer_sum_ratings_counts),
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_rating_count(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        # We count the rating of each movie, so we can return the movieID, and an integer, 
        # because we do not care about what rating it is, just if there is a rating, we can return 1.
        yield movieID, 1


    # MRJob automatically sorts data based on key value.
    # To make sure we can sort based on the value we write two functions.
    # A Combiner: combine_count_ratings. To sum up the amount of ratings given
    # And a Reducer: reducer_sum_ratings_counts, so we can remove the Key, and put the data we need in a list
    def combine_count_ratings(self, movieID, rating):
        yield movieID, sum(rating)

    def reducer_sum_ratings_counts(self, key, values):
        yield None, (sum(values), key)

    # This function sorts the List from reducer_sum_ratings_counts and yields the Key-Value pair again
    def reduce_sort_counts(self, _, movies):
        for value, key in sorted(movies, reverse=True):
            yield (key, int(value))

if __name__ == '__main__': 
    RatingsBreakdown.run()