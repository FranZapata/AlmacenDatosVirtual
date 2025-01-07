from re import S
import pymongo
from datetime import datetime

SORT_DICT = {"asc": pymongo.ASCENDING, "desc": pymongo.DESCENDING}

def connect(cfg): 
    return Connection(cfg)

class Connection:
    def __init__(self, cfg): 
        self.cfg = cfg
        self.con = pymongo.MongoClient(
            host=cfg["host"],
            port=cfg["port"]
        )


    def search(self, collection, fields=None, 
        query=None, sort=None, first=0, 
        count=None): 
        db = self.con[self.cfg["database"]]
        struct = db.estructure.find_one({'col': collection})
        struct_fields = {f['name']: f['type'] for f in struct['fields']} 
        col = db[collection]

        filt = self.format_query(struct_fields, query) 
        print(f"FILT: {filt} \nPROJECTION: {fields} \nLIMIT: {count} \nSORT: {sort} ")
        if fields is not None and "*" not in fields:
            cur = col.find(filt, projection = fields)
        else:
            cur = col.find(filt)
        
        if sort is not None: 
            cur.sort(sort[0], SORT_DICT[sort[1]])
        
        if count is not None: 
            cur.limit(int(count))
        
        return list(cur)


    def count(self, collection): 
        db = self.con[self.cfg["database"]] 
        col = db[collection]
        return col.count_documents()

    
    def close(self): self.con.close()

    # ------------------------------------  lab2
    def create(self, collection, fields): 
        db = self.con[self.cfg["database"]] 
        col = db[collection]
        db.create_collection(collection)

        # Insertamos la estructura de la coleccion en otra coleccion llamada estructura
        db.estructure.insert_one({
            'col': collection,
            'fields': fields
        })
        

    def destroy(self, collection): 
        db = self.con[self.cfg["database"]] 
        col = db[collection]
        db.estructure.delete_one({'col': collection})
        col.drop()

        

    def insert(self, collection, doc): 
        # Los datos de doc deben coincidir con los de la estructura 
        # de la coleccion en la que vamos a insertar
        db = self.con[self.cfg["database"]]
        estructure = db.estructure.find_one({'col': collection})
        col = db[collection]
        dic = {}
        cont = 0
        index = None
        for f in estructure['fields']:
            if len(doc) == cont: return "Invalid number of elements."        
            if f['name'] == "date":
                date = datetime.strptime(doc[cont].replace("'", ""), '%d/%m/%Y')
                dic[f['name']] = date.timestamp()
            else:
                dic[f['name']] = eval(f['type'])(doc[cont].replace("'", ""))

            if "primary" in f or "unique" in f:
                col.create_index(f['name'], unique=True)

            cont += 1

        col.insert_one(dic)
        return "Insert done."

    def update(self, collection, query, data): 
        db = self.con[self.cfg["database"]]
        struct = db.estructure.find_one({'col': collection})
        struct_fields = {f['name']: f['type'] for f in struct['fields']}
        col = db[collection]
        filt = self.format_query(struct_fields, query)
        update = {'$set':{}}
        for d in data:
            [dField, dValue] = d.split("=")
            update['$set'][dField] = eval(struct_fields[dField])(dValue)
        col.update_many(filt, update)
        return "Update done."

    def delete(self, collection, query): 
        db = self.con[self.cfg["database"]]
        struct = db.estructure.find_one({'col': collection})
        struct_fields = {f['name']: f['type'] for f in struct['fields']}
        col = db[collection]

        filt = self.format_query(struct_fields, query)
        col.delete_many(filt)
        return "Delete done."

    def format_query(self, struct_fields, query):
        # Util func para convertir las querys a filtros de mongodb
        if query is not None:
            filt = {}
            query_formatted = [s.replace("'", "") for s in query]             
            for q in query_formatted:
                q = q.strip()
                if "<=" in q: 
                    [qField, qValue] = q.split("<=")
                    filt[qField] = {'$lte':eval(struct_fields[qField])(qValue)}
                elif ">=" in q:
                    [qField, qValue] = q.split(">=")
                    filt[qField] = {'$gte':eval(struct_fields[qField])(qValue)}
                elif "<>" in q:
                    [qField, qValue] = q.split("<>")
                    filt[qField] = {'$neq':eval(struct_fields[qField])(qValue)}
                elif "<" in q:
                    [qField, qValue] = q.split("<")
                    filt[qField] = {'$lt':eval(struct_fields[qField])(qValue)}
                elif ">" in q:
                    [qField, qValue] = q.split(">")
                    filt[qField] = {'$gt':eval(struct_fields[qField])(qValue)}
                elif "=" in q:
                    [qField, qValue] = q.split("=")
                    filt[qField] = {'$eq':eval(struct_fields[qField])(qValue)}
            
            return filt





