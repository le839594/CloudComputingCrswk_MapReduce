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
            # if passenger not already in data to be returned, make new entry e.g. (passenger123: 1)
            if key[0:10] not in data:
                data[key[0:10]] = [v]
            # if passenger already in data to be returned, add 1 to passenger entry e.g. (passenger123: 1,1)
            else:
                data[key[0:10]].append(v)
        prev_key = key
    return data


# reduce the entry for each passenger e.g. (passenger123: 1,1) would become (passenger123: 2)
def reduce_func(x, y):
    return x+y


# process input dataset for input to mapper, the passenger_id and flight_id make up the key
map_in = []
for line in sys.stdin:
    line = line.strip()
    cols = line.split(',')
    passenger_id = cols[0]
    flight_id = cols[1]
    map_in.append(passenger_id + flight_id)


# running of functions, with logic to determine max_flights by a passenger
map_out = map(map_func, map_in)
reduce_in = shuffle(map_out)
reduce_out = {}

max_passenger = []
max_flights = 0
for key, values in reduce_in.items():
    try:
        for i in values:
            count = int(i)
    except ValueError:
        print("Value Error, discarding passenger")
        continue
    reduce_out[key] = reduce(reduce_func, values)
    if reduce_out[key] > max_flights:
        max_passenger = []
        max_passenger.append([key])
        max_flights = reduce_out[key]
    elif reduce_out[key] == max_flights:
        max_passenger.append([key])

# output the passenger(s) with most flights to a txt file
max_passengers_array = []
for i in max_passenger:
    max_passengers_array.append(i[0]+" ")
max_passengers_str = ''.join(max_passengers_array)

file = open("Task2_output.txt", "w")
file.write("The passenger(s) ID(s) with the most flights in the data are " + max_passengers_str + "with " + str(max_flights))
file.close
