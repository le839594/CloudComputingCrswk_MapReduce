import sys
import multiprocessing as mp


# the map function that takes the key as an input
def map_func(x):
    # determine what thread number is being run
    if mp.current_process().name == 'MainProcess':
        print('Running on main process')
    else:
        rank = mp.current_process()._identity[0]
        #print(f'Running on process: {rank}')
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
def reduce_func(x, *args):
    airport = x[0]
    numbers = x[1]
    total = sum(numbers)
    # determine what thread number is being run
    if mp.current_process().name == 'MainProcess':
        print('Running on main process')
    else:
        rank = mp.current_process()._identity[0]
        # print(f'Running on process: {rank}')
    return airport, total

# process input dataset for input to mapper, the flight_id and from_airport make up the key
map_in = []
for line in sys.stdin:
    line = line.strip()
    cols = line.split(',')
    flight_id = cols[1]
    from_airport = cols[2]
    key = flight_id + from_airport
    # error handling for key by checking its alphanumeric and length is 11 (8 + 3)
    # and checking middle 4 digits (of flight ID) are integers
    if key.isalnum() and len(key) == 11:
        try:
            id_check = int(key[3:7])
        except ValueError:
            print("ID Error, discarding line")
            break

        map_in.append(key)


if __name__ == '__main__':
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # use of multiprocessing in mapping operation
        map_out = pool.map(map_func, map_in, chunksize=int(len(map_in)/mp.cpu_count()))

        reduce_in = shuffle(map_out)
        # use of multiprocessing in reducing operation
        reduce_out = pool.map(reduce_func, reduce_in.items(), chunksize=int(len(reduce_in.keys())/mp.cpu_count()))
        # output airports and number of flights from each to a txt file
        airports = []
        j = 0
        for i in reduce_out:
            if j == 0:
                airports.append("\n" + i[0] + ":" + str(i[1]) + "\n")
                j = 1
            else:
                airports.append(i[0] + ":" + str(i[1]) + "\n")
        airports_str = ''.join(airports)
        airports_str = airports_str[:-1]

        file = open("Task1_output.txt", "w")
        file.write("The counts for flights from each airport are: " + airports_str)
        file.close