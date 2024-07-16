import kuzu

def execute_cypher_file(conn, file_path):
    with open(file_path, 'r') as file:
        for line in file:
            statement = line.strip()
            if statement:
                print(statement)
                conn.execute(statement)

def main():
    # Initialize database
    print("starting...")
    db = kuzu.Database("./job_db")
    conn = kuzu.Connection(db)

    print("start inserting")

    # Create schema
    execute_cypher_file(conn, "./utils/job_schema.cypher")

    # Insert data
    execute_cypher_file(conn, "./utils/job_dataloading.cypher")

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
        main()