from mrjob.job import MRJob
from mrjob.step import MRStep

"""
Name: Guido van Dijk
Student Nr. 648539
"""
class RatingsBreakdown (MRJob):     
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_rating_count,
                    reducer=self.reducer_sum_ratings_counts),
            # You cannot have multiple reducers in one step, so a second step is made to show the sorted data.
            # A Combiner could have been used to make everything in one step, but I think this is a bit cleaner
            MRStep(reducer=self.reduce_sort_counts)
        ]

    def mapper_get_rating_count(self, _, line):
        # Split items by every tab key and save them in their variables corresponding to the position.
        (userID, movieID, rating, timestamp) = line.split('\t')

        # We count the rating of each movie, so we can return the movieID, and an integer, 
        # because we do not care about what rating it is, just if there is a rating, we can return 1.
        yield movieID, 1

    # MRJob automatically sorts data based on the Key value.
    # To make sure we can sort based on the amount of ratings we yield a list with the data and a None as key, 
    # This way we can sort the list in the next step.
    def reducer_sum_ratings_counts(self, movieID, rating):
        yield None, (sum(rating), movieID)

    # This function sorts the List from reducer_sum_ratings_counts and yields the Key-Value pair again
    def reduce_sort_counts(self, _, movies):
        for rating, movieID in sorted(movies, reverse=True):
            yield (int(movieID), int(rating))

if __name__ == '__main__': 
    RatingsBreakdown.run()