import sys
import traceback
import virtualdb
from tabulate import tabulate

def help():
    print("""
    Usage: python3 cli.py <catalog.yaml>
    Available commands within the prompt
        <sql>: execute SQL query or statement
        .help: print help
        .create: create the virtual database
        .destroy: destroy the virtual database
        .describe: print virtual schema
        .loadData: loads demo data in tables
        .exit: close the current connection
    """)

if len(sys.argv) < 2 : help()
else:
    path = sys.argv[1]
    virtualdb.loadDrivers()
    virtualdb.loadCatalog(path)

    while True:
        cmd = input("virtual> ").split()
        if len(cmd) == 0:
            pass
        elif cmd[0] == ".help":
            help()
        elif cmd[0] == ".describe":
            desc = virtualdb.describe()
            print(desc)
        elif cmd[0] == ".create":
            virtualdb.create()
            print("database created.")
        elif cmd[0] == ".destroy":
            virtualdb.destroy()
            print("database destroyed.")
        elif cmd[0] == ".loadData":
            virtualdb.loadData()
            print("database loaded with demo data.")
        elif cmd[0] == ".exit":
            print("bye!")
            exit(0)
        else:
            # Asumimos que es una query
            sql= " ".join(cmd)
            try:
                if sql.startswith("select"):
                    res = virtualdb.query(sql)
                    if len(res)>0:
                        # Formatear la tabla usando libreria externa: tabulate
                        res = tabulate(res, headers="keys", tablefmt="grid")
                else:
                    res = virtualdb.execute(sql)
                print(res)
            except Exception as e:
                traceback.print_exc()
