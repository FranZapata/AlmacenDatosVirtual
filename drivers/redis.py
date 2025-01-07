import redis
import json
import re

def connect(cfg): 
    return Connection(cfg)

class Connection:
    def __init__(self, cfg): 
        self.cfg = cfg
        self.con = redis.Redis(
            host=cfg['host'], 
            port=cfg['port']
        )


    def search(self, collection, fields=None, 
        query=None, sort=None, first=0, 
        count=None): 
        
        # Cargamos la estructura de la tabla para convertir los datos al tipo correspondiente en los filtros
        key = f"/{self.cfg['database']}/{collection}"
        struct = self.con.get(key)
        struct = json.loads(struct)
        struct_fields = {f['name']: f['type'] for f in struct}

        keys = self.con.keys(f"/{self.cfg['database']}/{collection}/*")
        docs = [] 
        for key in keys:
            value = self.con.get(key)
            doc = json.loads(value)          

            #Aplicamos filtros query
            add = True
            if query is not None:  
                query_formatted = [s.replace("'", "") for s in query]             
                for q in query_formatted:
                    if add:
                        q = q.strip()
                        if "<=" in q: 
                            [qField, qValue] = q.split("<=")
                            add = doc[qField] <= eval(struct_fields[qField])(qValue)
                        elif ">=" in q: 
                            [qField, qValue] = q.split(">=")
                            add = doc[qField] >= eval(struct_fields[qField])(qValue)
                        elif "<>" in q: 
                            [qField, qValue] = q.split("<>")
                            add = doc[qField] != eval(struct_fields[qField])(qValue)
                        elif ">" in q: 
                            [qField, qValue] = q.split(">")
                            add = doc[qField] > eval(struct_fields[qField])(qValue)
                        elif "<" in q: 
                            [qField, qValue] = q.split("<")
                            add = doc[qField] < eval(struct_fields[qField])(qValue)
                        elif "=" in q: 
                            [qField, qValue] = q.split("=")
                            add = doc[qField] == eval(struct_fields[qField])(qValue)

            # Añadimos el doc con los campos que ha pedido el usuario            
            if fields is not None and "*" not in fields:
                doc_filtrado = []
                for field in fields: 
                    doc_filtrado.append(doc[field.strip()])
                doc = doc_filtrado
            if add:
                docs.append(doc)
        
        # Ordenar resultados
        if sort is not None:
            desc = True if sort[1] == "desc" else False
            sorted_docs = sorted(docs, key=lambda x: x[sort[0]], reverse=desc)
            docs = sorted_docs
        
        # Limitar resultados
        if count is not None:
            docs = docs[0:int(count)]

        return docs        


    def count(self, collection): 
        keys = self.con.keys(f"/{self.cfg['database']}/{collection}/*")
        return len(keys)

    
    def close(self): self.con.close()

    # ------------------------------------  lab2
    def create(self, collection, fields):
        key = f"/{self.cfg['database']}/{collection}"
        val = json.dumps(fields)
        self.con.set(key, val)

    def destroy(self, collection): 
        # Primero borramos contenido de la coleccion
        keys = self.con.keys(f"/{self.cfg['database']}/{collection}/*")
        for key in keys: self.con.delete(key)

        # Luego borramos la coleccion
        self.con.delete(f"/{self.cfg['database']}/{collection}")

    def insert(self, collection, doc): 
        # Los datos de doc deben coincidir con los de la estructura 
        # de la coleccion en la que vamos a insertar
        if len(self.search(collection=collection, query=[f"id={doc[0]}"]))> 0: return "El id ya existe"
        key = f"/{self.cfg['database']}/{collection}"
        struct = self.con.get(key)
        struct = json.loads(struct)
        fields = [[field['name'],field['type']] for field in struct]
        val = {}
        
        if len(doc) == len(fields):
            val = {fields[i][0]: eval(fields[i][1])(doc[i].replace("'", "")) for i in range(len(fields))}
            key = f"/{self.cfg['database']}/{collection}/{doc[0]}" 
            val = json.dumps(val)
            self.con.set(key, val)
        else:
            return "Number of fields doesnt match with number of collumns in table. Check it"
        return "Insert done."

    def update(self, collection, query, data): 
        docs = self.search(collection, query=query)
        for doc in docs:
            for d in data:
                [qField, qValue] = d.split("=")
                doc[qField] = qValue
            key = f"/{self.cfg['database']}/{collection}/{doc['id']}" 
            val = json.dumps(doc)
            self.con.set(key, val)
        return "Update done."

    def delete(self, collection, query): 
        docs = self.search(collection, query=query)
        for doc in docs:
            key = f"/{self.cfg['database']}/{collection}/{doc['id']}" 
            self.con.delete(key)
        return "Delete done."




