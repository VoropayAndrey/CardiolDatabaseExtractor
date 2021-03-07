# Cardiol database extractor
# Version 1.1.0
# Author: Andrey Voropay

import sqlite3
import sys
import zlib
import time
import os
from enum import Enum


class Mode(Enum):
    VIEW = 0
    EXTRACT = 1


class ExtractMode(Enum):
    EXTRACT_ALL = 0
    EXTRACT_BY_ID = 1


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    currentMode = Mode.EXTRACT
    currentExtractMode = ExtractMode.EXTRACT_ALL
    idToExtractList = []
    # idFlagIndex = 0

    print("Python version: ", sys.version)
    print("sqlite3 version: ", sqlite3.version)

    argv_lenght = len(sys.argv)

    print("Number of input parameters: ", argv_lenght - 1)

    output_folder = "./"

    for i in range(argv_lenght):
        if isinstance(sys.argv[i], str) and sys.argv[i] == '-v':
            currentMode = Mode.VIEW
            continue
        if isinstance(sys.argv[i], str) and sys.argv[i] == '-id':
            currentExtractMode = ExtractMode.EXTRACT_BY_ID
            idFlagIndex = i
            continue
        if isinstance(sys.argv[i], str) and currentExtractMode == ExtractMode.EXTRACT_BY_ID:
            idToExtractList.append(sys.argv[i])
            continue
        if isinstance(sys.argv[i], str) and i == 1:
            databaseFile = sys.argv[i]
            print("Input database file: ", databaseFile)
            continue
        if isinstance(sys.argv[i], str) and i == 2:
            output_folder = sys.argv[i]
            if os.path.isdir(output_folder):
                print("Output folder: ", output_folder)
            else:
                try:
                    os.mkdir(output_folder)
                except OSError:
                    print("Can't create output folder: ", output_folder)
                else:
                    print("Output folder created: ", output_folder)
            continue

    # databaseFile = "Data/TestDB.cDelver"
    print("Mode: " + str(currentMode))
    if (currentMode == Mode.EXTRACT):
        print("Extract mode: " + str(currentExtractMode))
        if currentExtractMode == ExtractMode.EXTRACT_BY_ID:
            print("ID list: " + str(idToExtractList))
    sqlite_connection = sqlite3.connect(databaseFile)
    try:
        query = ''
        cursor = sqlite_connection.cursor()
        if currentExtractMode == ExtractMode.EXTRACT_ALL:
            query = 'SELECT * FROM records'
        elif currentExtractMode == ExtractMode.EXTRACT_BY_ID:
            idListString = ""
            idCount = 0
            for id in idToExtractList:
                idListString += id
                idCount += 1
                if idCount < len(idToExtractList):
                    idListString += ','
            query = 'SELECT * FROM records WHERE id IN (' + idListString + ')'
        result = cursor.execute(query)
        result_set = cursor.fetchall()
        cursor.close()
        sqlite_connection.close()
        for row in result_set:
            raw_date = int(((row[2] - 25569) * 86400))
            local_time = time.gmtime(raw_date)
            record_date = time.strftime('%Y-%m-%dT%H:%M:%SZ', local_time)
            number_of_channels = row[4]
            data = row[6]
            sample_count = row[5]

            percent_cout = 0
            samples_written_count = 0
            samples_per_one_percent = sample_count / 100.0

            print("ID: ", row[0],
                  ", Patient ID: ", row[1],
                  ", Date: ", record_date,
                  ", Sample rate: ", row[3],
                  ", Channel count", number_of_channels,
                  ", Sample count", sample_count)

            if (currentMode == Mode.EXTRACT_MODE):

                # Create an output filename
                output_filename = output_folder + "/" + record_date + "-" + str(sample_count) + ".csv"

                decompressed_data = zlib.decompress(data)
                decompressed_data_length = len(decompressed_data)

                # Create a list for all channels, [j][i] where j is a channel index (0-11 in a 12 channel record), i is a sample index
                samples_list = [[] for i in range(number_of_channels)]

                for j in range(0, decompressed_data_length, 4 * number_of_channels):
                    for i in range(number_of_channels):
                        amplitude = int.from_bytes(bytes=[decompressed_data[(i * 4) + j],
                                                          decompressed_data[(i * 4) + j + 1],
                                                          decompressed_data[(i * 4) + j + 2],
                                                          decompressed_data[(i * 4) + j + 3]],
                                                   byteorder='little',
                                                   signed=True)
                        samples_list[i].append(amplitude)

                # Write samples to CSV files
                with open(output_filename, "w") as f:
                    for index in range(len(samples_list[0])):
                        csv_string = ""
                        for channel in range(len(samples_list)):
                            csv_string += str(samples_list[channel][index])
                            if channel < len(samples_list) - 1:
                                csv_string += ","
                        if index < len(samples_list[0]) - 1:
                            csv_string += "\n"
                        f.write(csv_string)
                        samples_written_count += 1
                        if samples_written_count >= samples_per_one_percent:
                            samples_written_count = 0
                            percent_cout += 1
                            print(".", end='')
                    print("\nOutput file created: " + output_filename)
                f.close()
    except Exception as e:
        print("Exception!\n", e)
    else:
        print("Done!")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
