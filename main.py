import sqlite3

def Extraction():
    import csv
    from datetime import datetime
    
    file = open('databus.csv')
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    rows = []
    for row in csvreader:
        rows.append(row)
    file.close()
    con = sqlite3.connect('data_bus.db')
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS DATABUS   
            (ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            ROUTE TEXT NOT NULL,
            DATE TIMESTAMP NOT NULL,
            DAYTYPE TEXT NOT NULL,
            RIDES INTEGER);''')
    data_tuple = []

    for data in rows:
        a = data[1].split('/')
        data_tuple.append((data[0],datetime(month=int(a[0]),day=int(a[1]),year=int(a[2])),data[2],int(data[3])))
    qry = ('''INSERT INTO DATABUS ('ROUTE','DATE','DAYTYPE','RIDES') VALUES (?,?, ?, ?);''')
    cur.executemany(qry, data_tuple)
    con.commit()
    
    cur.close()
    con.close()

def average():
    con = sqlite3.connect('data_bus.db')
    cur = con.cursor()
    
    qry = ('''SELECT 
        DATE,
        AVG(RIDES)
    FROM 
        DATABUS
    GROUP BY 
        date''')
    cur.execute(qry)

    records = cur.fetchall()
    cur.close()
    con.close()
    return records


def average_dt(daytypes = ["W","A","U"]):
    con = sqlite3.connect('data_bus.db')
    cur = con.cursor()
    records = []
    for dt in daytypes:
        qry = ('''SELECT 
            DATE,
            daytype,
            avg(rides)
        FROM 
            DATABUS
        Where daytype = ?
        GROUP BY 
            date''')
        cur.execute(qry,(dt))
        record = cur.fetchall()
        records.extend(record)
    cur.close()
    con.close()
    return records

def fix():
    con = sqlite3.connect('data_bus.db')
    cur = con.cursor()    
    qry = ('''SELECT 
         ROUTE,DATE,DAYTYPE,RIDES
    FROM 
         DATABUS
         ''')
    cur.execute(qry)
    records = cur.fetchall()
    import os
    if os.path.exists("data_bus_backup.db"):
        os.remove("data_bus_backup.db")

    con2 = sqlite3.connect('data_bus_backup.db')
    cur2 = con2.cursor()
    cur2.execute('''CREATE TABLE IF NOT EXISTS DATABUS   
            (ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            ROUTE TEXT NOT NULL,
            DATE TIMESTAMP NOT NULL,
            DAYTYPE TEXT NOT NULL,
            RIDES INTEGER);''')
    
    data_tuple = []
    for data in records:
        data_tuple.append((data[0],data[1],data[2],int(data[3])))
    qry = ('''INSERT INTO DATABUS ('ROUTE','DATE','DAYTYPE','RIDES') VALUES (?,?, ?, ?);''')
    cur2.executemany(qry, data_tuple)
    con2.commit()
    cur2.close()
    con2.close()
    
    qry = ('''SELECT 
         ID,RIDES
    FROM 
         DATABUS
        Where strftime('%d', date) = '01' AND route = 3''')
    
    cur.execute(qry)
    records = cur.fetchall()
    values = []
    for record in records:
        values.append((record[1]+10,record[0]))
    qry2 = ('''UPDATE DATABUS set RIDES=? where ID = ?''')
    cur.executemany(qry2,values)  
    row_effected = cur.rowcount
    con.commit()
#     print(row_effected)
    cur.close()
    con.close()

def covid_19():
    con = sqlite3.connect('data_bus.db')
    cur = con.cursor()
    
    qry = ('''SELECT AVG(RIDES) FROM DATABUS 
WHERE strftime('%Y-%m', date) >= "2020-03"''')
    cur.execute(qry)
    after = cur.fetchone()
    
    qry = ('''SELECT AVG(RIDES) FROM DATABUS 
WHERE strftime('%Y-%m', date) < "2020-03"''')
    cur.execute(qry)
    before = cur.fetchone()
    
    cur.close()
    con.close()
    print("Average of Rides before pandamic was : ",round(before[0]))
    print("Average of Rides in pandamic is : ",round(after[0]))
    if before[0]>=after[0]:
        print("Rides are decrease in pandamic")
    else:
        print("Rides are increase in pandamic")
        
    

# Function calling
Extraction()
# -----------------------------------------------------------------------

records = average()
for record in records:
    print(f"{record[0]} has the average of {round(record[1])} rides")

# -----------------------------------------------------------------------
records = average_dt()
for record in records:
    print(f"For {record[1]} daytype has the average of {round(record[2])} rides on {record[0]}")

# -----------------------------------------------------------------------
fix()

# -----------------------------------------------------------------------
covid_19()
