if __name__ == "__main__":
    rows = int(input("How many rows to sample? "))
    inFile = open("flightrows_720mb.csv", "r")
    outFile = open("demorows.csv", "w")
    for i in range(rows):
        outFile.write(inFile.readline())
    inFile.close()
    outFile.close()