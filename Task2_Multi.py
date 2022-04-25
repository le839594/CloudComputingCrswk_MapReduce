import sys
import multiprocessing as mp

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
def reduce_func(x, *args):
    passenger = x[0]
    numbers = x[1]
    total = sum(numbers)
    return passenger, total


#process input dataset for input to mapper, the passenger_id and flight_id make up the key
map_in = []
for line in sys.stdin:
    line = line.strip()
    # Split the line into records - list of columns
    cols = line.split(',')
    passenger_id = cols[0]
    flight_id = cols[1]
    map_in.append(passenger_id + flight_id)

if __name__ == '__main__':
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # use of multiprocessing in mapping operation
        map_out = pool.map(map_func, map_in, chunksize=int(len(map_in)/mp.cpu_count()))
        reduce_in = shuffle(map_out)
        # use of multiprocessing in reducing operation
        reduce_out = pool.map(reduce_func, reduce_in.items(), chunksize=int(len(reduce_in.keys()) / mp.cpu_count()))
        max_flights = 0
        max_passenger = []
        # logic to determine max_flights by a passenger
        for i in reduce_out:
            if i[1] > max_flights:
                max_passenger = []
                max_passenger.append(i[0])
                max_flights = i[1]
            elif i[1] == max_flights:
                max_passenger.append(i[0])
        # output the passenger(s) with most flights to a txt file
        max_passengers_array = []
        print(max_passenger)
        j = 0
        for i in max_passenger:
            if j == 0:
                max_passengers_array.append(i[:])
                j = 1
            else:
                max_passengers_array.append(", " + i[:])
        max_passengers_str = ''.join(max_passengers_array)

        file = open("Task2_output.txt", "w")
        file.write("The passenger(s) ID(s) with the most flights in the data are " + max_passengers_str + " with " + str(
            max_flights) + " flights.")
        file.close

