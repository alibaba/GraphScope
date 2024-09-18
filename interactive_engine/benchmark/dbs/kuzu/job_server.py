import kuzu
import sys

def execute_cypher_file(conn, file_path):
    with open(file_path, 'r') as file:
        for line in file:
            statement = line.strip()
            if statement:
                conn.execute(statement)

def main(db_path, schema_path, dataloading_path):
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    print("start inserting")

    # Create schema
    execute_cypher_file(conn, schema_path)

    # Insert data
    execute_cypher_file(conn, dataloading_path)

    print("start querying")

    # Execute Cypher query
    response = conn.execute(
        """
        MATCH (a:NAME)
        RETURN count(a)
        """
    )

    while response.has_next():
        print(response.get_next())

    print("query end")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <db_path> <schema_path> <dataloading_path>")
    else:
        db_path = sys.argv[1]
        schema_path = sys.argv[2]
        dataloading_path = sys.argv[3]
        main(db_path, schema_path, dataloading_path)