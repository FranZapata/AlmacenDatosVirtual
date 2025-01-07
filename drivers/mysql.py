import mysql.connector

TYPES = {
    "str": "varchar(255)",
    "int": "int",
    "float": "float(10,2)",
    "date": "date"
    
}
def connect(cfg): 
    return Connection(cfg)

class Connection:
    def __init__(self, cfg): 
        self.cfg = cfg
        self.con = mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )


    def search(self, collection, fields=None, 
        query=None, sort=None, first=0, 
        count=None): 

        fields_str = ""
        if fields is not None:
            fields_str = ",".join(fields)
        sql = f"SELECT {fields_str} FROM {collection}"
        if query:
            where_clause = " AND ".join(query)
            sql += f" WHERE {where_clause}"
        if sort:
            sort_clause = " ".join(sort)
            sql += f" ORDER BY {sort_clause}"
        if count:
            sql += f" LIMIT {count}"
        cur = self.con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        docs = []
        for row in rows:
            doc = {}
            for i in range(len(cur.column_names)):
                col = cur.column_names[i]
                doc[col] = row[i]
            docs.append(doc)
        return docs



    def count(self, collection): 
        cur = self.con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {collection}")
        ret = cur.fetchall()
        return ret[0][0]  

    
    def close(self): self.con.close()

# ------------------------------------  lab2
    def create(self, collection, fields):
        cur = self.con.cursor()
        primary_key = ""
        query = f"CREATE TABLE IF NOT EXISTS {collection} ( "
        for field in fields:
            query += f"{field['name']} {TYPES[field['type']]}, "
            if "primary" in field: primary_key = f" ,PRIMARY KEY ({field['name']}) );"
        query = query[:-2] # Quitar ultima coma
        query += primary_key
        cur.execute(query)
         
    
    def destroy(self, collection): 
        cur = self.con.cursor()
        cur.execute(f"DROP TABLE {collection}")

    def insert(self, collection, doc): 
        cur = self.con.cursor()

        query = f"INSERT INTO {collection} values ("
        for d in doc:
            query += f"{d}, "
        query = query[:-2] # Quitar ultima coma
        query += " );"

        cur.execute(query)
        self.con.commit()
        return "Insert done."

    def update(self, collection, query, data): 
        cur = self.con.cursor()
        values = ", ".join(data)

        query = f"UPDATE {collection} SET {values} "
        if len(query)>0:
            where_clause = " AND ".join(query)
            query += f"WHERE {where_clause};"

        cur.execute(query)
        self.con.commit()
        return "Update done."
        

    def delete(self, collection, query): 
        cur = self.con.cursor()

        query_d = f"DELETE FROM {collection} "
        if len(query)>0:
            where_clause = " AND ".join(query)
            query_d += f"WHERE {where_clause};"
            
        cur.execute(query_d)
        self.con.commit()
        return "Delete done."



