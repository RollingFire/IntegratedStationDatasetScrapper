'''
By Austin Dorsey
Started: 5/22/18
Last Modified: 6/4/18
Discription: Handles and manages the Intedrated Station Database Stataions. 
             Converts the raw file into the a list of dictionaried stations. 
             Also provides filtering functions for the stations.
'''

from collections import defaultdict


def fileToStations(file):
    '''Turns the raw file into a list of the stations. With each eliment being
    a dictionary of the stations fields.
    '''
    stations = []
    keys = [x.replace('"', '') for x in file[0].split('","')]
    for line in file[1:]:
        dictionary = defaultdict(str)
        for key, val in zip(keys, line.split(',')):
            dictionary[key] = val.replace('"', '')
        stations.append(dictionary)
    return stations


def filterStations(stations, sortFn, arg):
    '''Filters the stations out with the given sort function and args.'''
    return [station for station in stations if sortFn(station, arg)]


def filterStationByString(station, arg):
    '''Checks to see if the data column (arg[0]) in station, is equal to arg[1].'''
    try:
        if station[arg[0]] == arg[1]:
            return True
        else:
            return False
    except KeyError:
        print("Invalid arg. Key", arg[0], "does not exist.")
    except IndexError:
        print("Invalid arg.", arg, "for filter by string.")
    return True


def filterStationByRange(station, arg):
    '''Checks to see if the data column (arg[0]) in station, is in the provided range.
    (arg[1] and arg[2])
    '''
    try:
        if arg[0] == "TIME":
            print("TIME sorting, is curently unsuported.")
            return True
        if arg[0] == "USAF":
            if station[arg[0]].isalpha():
                station[arg[0]][0] = ord(station[arg[0]][0])
        for i in range(2):
            if isinstance(arg[i + 1], str):
                if arg[i + 1][0].isalpha():
                    arg[i + 1] = float(str(ord(arg[i + 1][0])) + str(arg[i + 1][1:]))
                else:
                    arg[i + 1] = float(arg[i + 1])
        if station[arg[0]] == '':
            return False
        if (customStrToFloat(station[arg[0]]) >= min(arg[1:3]) and 
            customStrToFloat(station[arg[0]]) <= max(arg[1:3])):
            return True
        else:
            return False
    except KeyError:
        print("Invalid arg. Key", arg[0], "does not exist.", station)
    except IndexError:
        print("Invalid arg.", arg, "for filer by range.")
    except ValueError as err:
        print(err)
    except Exception as err:
        print(err)
    return True


def customStrToFloat(x):
    '''Converts strings with +/- to floats'''
    try:
        return float(x)
    except:
        negitive = 1
        if x.count('-') != 0:
            negitive = -1
            x = x.replace('-', '')
        x = x.replace('+', '')
        x = x.replace("'", '')
        return float(x) * negitive
