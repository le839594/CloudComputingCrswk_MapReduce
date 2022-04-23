import sys
from functools import reduce

# the map function that takes the key as an input
def map_func(x):
    return (x, 1)


# shuffle function to sort the mapped values by key
def shuffle(mapper_out):
    data = {}
    prev_key = ""
    # sort according to key
    sorted_mapper_out = sorted(mapper_out, key=lambda tup: tup[0])
    for k, v in sorted_mapper_out:
        key = k
        if prev_key != key:
            # if passenger not already in data to be returned, make new entry e.g. (airport123: 1)
            if key[-3:] not in data:
                data[key[-3:]] = [v]
            # if passenger already in data to be returned, add 1 to passenger entry e.g. (airport123: 1,1)
            else:
                data[key[-3:]].append(v)
        prev_key = key
    return data


# reduce the entry for each passenger e.g. (airport123: 1,1) would become (airport123: 2)
def reduce_func(x, y):
    return x+y


# process input dataset for input to mapper, the flight_id and from_airport make up the key
map_in = []
for line in sys.stdin:
    line = line.strip()
    cols = line.split(',')
    flight_id = cols[1]
    from_airport = cols[2]
    map_in.append(flight_id + from_airport)

map_out = map(map_func, map_in)

reduce_in = shuffle(map_out)

reduce_out = {}
for key, values in reduce_in.items():
    reduce_out[key] = reduce(reduce_func, values)

# output airports and number of flights from each one
print(reduce_out)




