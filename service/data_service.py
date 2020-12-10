import csv
import json


class FileService:

    def read_csv_file(self, filename: str) -> list:
        with open(filename, "rt", encoding="cp850") as file:
            lineReader = csv.reader(file, delimiter=',', quotechar="\"")
            return [row for row in lineReader]

    def write_csv_file(self, data: list, filename: str):
        with open(filename, 'w', newline='', encoding="cp850") as file:
            linewriter = csv.writer(file, delimiter=',', quotechar="\"", quoting=csv.QUOTE_ALL)
            for row in data:
                try:
                    linewriter.writerow(row)
                except Exception as e:
                    print("Could not write csv file.", e)


class JsonService:

    def read_json(self, filename):
        with open(filename) as json_file:
            data = json.load(json_file)
            return data
