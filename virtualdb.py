import importlib
import re
import yaml
import json

drivers = {}
catalog = {}


def loadDrivers(path="./drivers"):
    
    # Variable global. Si no especificamos global, aunque se llamenigual, crea 
    # una copia local dentro de la funcion.
    # Cuando la funcion acaba, la variable se destruye

    global drivers
    drivers['redis'] = importlib.import_module('drivers.redis')
    drivers['mysql'] = importlib.import_module('drivers.mysql')
    drivers['mongo'] = importlib.import_module('drivers.mongo')

def loadCatalog(path):
    global catalog

    f = open(path,"rt")
    text = f.read()
    f.close()

    catalog = yaml.load(text, yaml.Loader)
    for tName in catalog:
        table = catalog[tName]
        if "fields" not in table: raise Exception(f"Inavlid catalog: missing fields in table {tName}")
        if "mapping" not in table: raise Exception(f"Inavlid catalog: missing mapping in table {tName}")

        for field in table["fields"]:
            if "name" not in field: raise Exception(f"Inavlid catalog: missing field name in table {tName}")
            if "type" not in field: raise Exception(f"Inavlid catalog: missing field type in table {tName}")
            # Seguir validando si el tipo es soportado, si el mapping es correcto, etc

def query(sql):
    sql = sql.strip().lower()

    # Filtramos el tipo de consulta (select, insert, update, delete)
    if sql.startswith("select"):
        # Construimos consulta select
        # Preparamos los datos de derecha a izquierda.
        # Primero separamos el offset, limit, order by, luego el where, luego el from y finalmente el select
        offset = None
        limit = None
        order = None
        where = None
        tName = None
        fields = None

        sql = sql.split("offset")
        if len(sql) > 1:
            offset = sql[1].strip()

        sql = sql[0].split("limit")
        if len(sql) > 1:
            limit = sql[1].strip()

        sql = sql[0].split("order by")
        if len(sql) > 1:
            order = sql[1].strip().split(" ")

        sql = sql[0].split("where")
        if len(sql) > 1:
            where = sql[1].strip()
            where_cleaned = re.sub(r"[()]", "", where)
            where = [value.strip() for value in where_cleaned.split("and")]


        sql = sql[0].split("from")
        if len(sql) > 1:
            tName = sql[1].strip()

        sql = sql[0].split("select")
        if len(sql) > 1:
            fields = sql[1].strip().split(",")
        
        table = catalog[tName]
        mapping = table["mapping"]

        con = drivers[mapping["driver"]].connect(mapping)
        results = con.search(mapping["collection"], fields = fields, query = where, sort = order, count = limit)
        return results


def describe():
    global catalog
    return json.dumps(catalog, indent=4, ensure_ascii=False)


# ------------------------ lab2
def create():
    global catalog
    global drivers
    for tName in catalog:
        table = catalog[tName]
        mapping = table['mapping']
        collection = mapping['collection']

        con = drivers[mapping['driver']].connect(mapping)
        con.create(collection, table['fields'])
        con.close()


def destroy(): 
    global catalog
    global drivers
    for tName in catalog:
        table = catalog[tName]
        mapping = table['mapping']
        collection = mapping['collection']
        con = drivers[mapping['driver']].connect(mapping)
        con.destroy(collection)
        con.close()

def execute(sql):
    sql = sql.strip().lower()

    if sql.startswith("insert"):
        doc = []
        tName = ""
        sql = sql.split("values")
        if len(sql) > 1:
            values = sql[1].strip()
            values_cleaned = re.sub(r"[()]", "", values)
            doc = [value.strip() for value in values_cleaned.split(",")]
        
        sql = sql[0].split("into")
        if len(sql) > 1:
            tName = sql[1].strip()

        table = catalog[tName]
        mapping = table["mapping"]

        con = drivers[mapping["driver"]].connect(mapping)
        results = con.insert(mapping["collection"], doc)
        return results

    elif sql.startswith("update"):
        # Construimos consulta update
        query = []
        data = []
        tName = ""
        sql = sql.split("where")
        if len(sql) > 1:
            query = sql[1].strip()
            query_cleaned = re.sub(r"[()']", "", query)
            query = [value.strip() for value in query_cleaned.split("and")]
        
        sql = sql[0].split("set")
        if len(sql) > 1:
            data = sql[1].strip()
            data_cleaned = re.sub(r"[']", "", data)
            data = [value.strip() for value in data_cleaned.split(",")]
        
        sql = sql[0].split("update")
        if len(sql) > 1:
            tName = sql[1].strip()
        table = catalog[tName]
        mapping = table["mapping"]

        con = drivers[mapping["driver"]].connect(mapping)
        results = con.update(mapping["collection"], query, data)
        return results
    elif sql.startswith("delete"):
        # Construimos consulta delete
        query = []
        tName = ""

        sql = sql.split("where")
        if len(sql) > 1:
            query = sql[1].strip()
            query_cleaned = re.sub(r"[()']", "", query)
            query = [value.strip() for value in query_cleaned.split("and")]

        sql = sql[0].split("from")
        if len(sql) > 1:
            tName = sql[1].strip()
        
        table = catalog[tName]
        mapping = table["mapping"]

        con = drivers[mapping["driver"]].connect(mapping)
        results = con.delete(mapping["collection"], query)
        return results

def loadData():
    # Metodo personalizado para hacer una carga de datos de ejemplo
    # USERS
    execute("delete from users")
    execute("insert into users values (1, 'pepe', 'pepe@emai.com')")
    execute("insert into users values (2, 'manolo', 'manolo@emai.com')")
    execute("insert into users values (11, 'rosa', 'rosa@emai.com')")
    execute("insert into users values (21, 'mar', 'mar@emai.com')")

    # CONTACTS
    execute("delete from contacts")
    execute("insert into contacts values (1, 'Contact1', 'Contact1@email.com', 1)")
    execute("insert into contacts values (2, 'Contact2', 'Contact2@email.com', 2)")
    execute("insert into contacts values (3, 'Contact3', 'Contact3@email.com', 11)")
    execute("insert into contacts values (4, 'Contact4', 'Contact4@email.com', 21)")
    execute("insert into contacts values (5, 'Contact5', 'Contact5@email.com', 1)")
    execute("insert into contacts values (6, 'Contact6', 'Contact6@email.com', 2)")
    execute("insert into contacts values (7, 'Contact7', 'Contact7@email.com', 11)")
    execute("insert into contacts values (8, 'Contact8', 'Contact8@email.com', 21)")

    # MESSAGES
    execute("insert into messages values (1, 'Titulo mensaje', 'Esto es un mensaje de prueba', '07/12/2024', 1, 2)")
    execute("insert into messages values (2, 'Titulo 2 mensaje', 'Esto es un mensaje de prueba', '07/05/2024', 2, 1)")
    execute("insert into messages values (3, 'Titulo 3 mensaje', 'Esto es un mensaje de prueba', '07/12/2024', 11, 21)")
    execute("insert into messages values (4, 'Titulo 4 mensaje', 'Esto es un mensaje de prueba', '23/12/2024', 21, 11)")
    execute("insert into messages values (5, 'Titulo 5 mensaje', 'Esto es un mensaje de prueba', '07/12/2023', 1, 21)")
