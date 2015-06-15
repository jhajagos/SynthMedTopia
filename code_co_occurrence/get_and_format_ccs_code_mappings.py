__author__ = 'janos'

"""This is a simple program to download and clean the CSV file made available for CCS codes"""

CCS_link = "http://www.hcup-us.ahrq.gov/toolssoftware/ccs/Single_Level_CCS_2015.zip"
import urllib2
import zipfile
import os
import csv

def main():
    print("")
    print("Downloading CCS zip file")
    try:
        f = urllib2.urlopen(CCS_link)
    except:
        print("URL '%s' could not be downloaded" % CCS_link)
        raise

    CCS_zip_file = str(CCS_link.split("/")[-1])
    print("Writing '%s'" % CCS_zip_file)
    data = f.read()
    with open(CCS_zip_file, "wb") as fw:
        fw.write(data)

    zip_ccs = zipfile.ZipFile(os.path.join(os.path.curdir, CCS_zip_file))
    files_written = []

    for name in zip_ccs.namelist():
        cf = zip_ccs.open(name,"r")
        data = cf.read()

        base_directory = os.path.join(os.path.curdir, "data")
        if not(os.path.exists(base_directory)):
            os.makedirs(base_directory)

        file_name_to_write = os.path.join(base_directory, name)
        with open(file_name_to_write, "wb") as fw:
            fw.write(data)
            files_written += [file_name_to_write]

    for file_name in files_written:
        print(file_name)

        with open(file_name, "rb") as fc:
            if "label" in file_name:
                cr = csv.reader(fc)
            else:
                cr = csv.reader(fc, quotechar="'")
                cr.next()

            i = 0

            result = []
            for line in cr:
                cleaned_line = []
                for cell in line:
                    if len(cell):
                        if cell[0] == '"' and cell[-1] == '"':
                            cleaned_cell = cell[1:-1]
                        else:
                            cleaned_cell = cell
                    else:
                        cleaned_cell = cell
                    cleaned_line += [cleaned_cell.rstrip()]
                result += [cleaned_line]
                i += 1


        directory, file_read = os.path.split(os.path.abspath(file_name))

        if "$" == file_read[0]:
            new_file_name = "cleaned_" + "_".join(file_read[1:].split(".")[0].split()) + ".csv"
        else:
            new_file_name = "cleaned_" + "_".join(file_read.split(".")[0].split()) + ".csv"

        full_path_new_file_name = os.path.join(os.path.curdir, "data", new_file_name)

        with open(full_path_new_file_name, "wb") as fw:
            cw = csv.writer(fw)
            for row in result:
                cw.writerow(row)

if __name__ == "__main__":
    main()