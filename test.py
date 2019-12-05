import os
import time


def test():
    inputdir = "/share/data5/hegh/project1/7.24/das_flow/8.5/testdir/"
    time2 = os.path.getatime(inputdir)
    time3 = os.path.getctime(inputdir)
    time4 = os.path.getmtime(inputdir)
    # time1 = os.stat(inputdir)
    # print(time)
    print(time2)
    print(time3)
    print(time4)


def read_samplesheetused_data():
    samplesheetused_path = "/share/data4/illumina/190715_MN00302_0284_A000H2VKFF/Alignment_1/20190716_141831/SampleSheetUsed.csv"
    dir_path = "/share/data4/illumina/190715_MN00302_0284_A000H2VKFF/Alignment_1/20190716_141831/Fastq"
    file_path = os.listdir(dir_path)
    sampale_name_list = {sname.split("_")[0]: 1 for sname in file_path}
    fasta_name = []
    flag = False
    if os.path.exists(samplesheetused_path):
        with open(samplesheetused_path, 'r') as pf:
            for line in pf:
                line_list = line.strip("\n").split(",")
                if len(line_list) == 6:
                    if line_list[0] != "Sample_ID":
                        fasta_name.append(line_list[1])
        print(fasta_name)
        print(sampale_name_list)
        try:
            for name in fasta_name:
                num = sampale_name_list[name]
            flag = True
        except:
            pass
    print(flag)
    return flag


if __name__ == '__main__':
    read_samplesheetused_data()
    # while 1:
    #     test()
    #     print("#"*100)
    #     time.sleep(1)
