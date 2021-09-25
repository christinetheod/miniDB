import database
import re
from database import Database

db = Database('vsmdb', load=False)

# Create the tables
db.create_table('classroom', ['building', 'room_number', 'capacity'], [str, int, int], primary_key='room_number')
db.insert('classroom', ['Packard', '101', '500'])
db.insert('classroom', ['Painter', '514', '10'])
db.insert('classroom', ['Taylor', '3128', '70'])
db.insert('classroom', ['Watson', '100', '30'])
db.insert('classroom', ['Watson', '120', '50'])

sql = ""
while sql != "exit":

    sql = input(
        "\nType a query or 'exit' for exit\n\n")

    dispatcher = {'str': str, 'int': int}
    table = ""  # table's name
    sql_splitted = sql.split(" ")  # command type

    if sql_splitted[0] == "select":  # select
        select_split = sql.split("select")
        after_from = select_split[1].split("from")
        if "top" not in sql_splitted:
            if "into" not in sql_splitted:
                columns = after_from[0].replace(" ", "")
                into = None
            else:  # command into
                into_split = after_from[0].split("into")
                columns = into_split[0].replace(" ", "")
                into = into_split[1].replace(" ", "")
            top = None
        else:  # command top
            temp = after_from[0].split(" ")
            top = int(temp[2])  # Value of top (ex. top 3)
            temp1 = after_from[0].split(str(top))
            columns = temp1[1].replace(" ", "")
            if "into" in sql_splitted:  # command into
                into_split = temp1[1].split("into")
                into = into_split[1].replace(" ", "")
                columns = into_split[0].replace(" ", "")
            else:
                into = None
        if "where" not in sql_splitted:
            if "order" not in sql_splitted and "by" not in sql_splitted:
                if table != "testtable":
                    table = after_from[1].replace(" ", "")
                if columns == "*":
                    db.select(table, columns, None, None, False, top, into)
                    # Show the table that was saved as...
                    if into is not None:
                        db.show_table(into)
                else:  # command order
                    db.select(table, columns.split(","), None, None, False, top, into)
                    if into is not None:
                        db.show_table(into)
            else:  # command order by
                after_order_by = after_from[1].split("order by")
                table = after_order_by[0].replace(" ", "")
                if "asc" not in sql_splitted:
                    order_by_clm = (after_order_by[1].replace("desc", "")).replace(" ",
                                                                                   "")
                    asc = False
                else:  # asc
                    order_by_clm = (after_order_by[1].replace("asc", "")).replace(" ",
                                                                                  "")
                    asc = True
                if columns == "*":
                    db.select(table, columns, None, order_by_clm, asc, top, into)
                    if into is not None:
                        db.show_table(into)
                else:
                    db.select(table, columns.split(","), None, order_by_clm, asc, top, into)
                    if into is not None:
                        db.show_table(into)
        else:  # command where
            if "order" not in sql_splitted and "by" not in sql_splitted:
                tmp = after_from[1].split("where")
                table = tmp[0].replace(" ", "")
                where_condition = tmp[1].replace(" ", "")
                if columns == "*":
                    db.select(table, columns, where_condition, None, False, top, into)
                    if into is not None:
                        db.show_table(into)
                else:
                    db.select(table, columns.split(","), where_condition, None, False, top, into)
                    if into is not None:
                        db.show_table(into)

            else:  # There is order by in the command
                tmp = after_from[1].split("where")
                table = tmp[0].replace(" ", "")
                tmp1 = tmp[1].split("order by")
                where_condition = tmp1[0].replace(" ", "")
                if "asc" not in sql_splitted:
                    order_by_clm = (tmp1[1].replace("desc", "")).replace(" ", "")  # tmp1[1] = 'column'
                    asc = False
                else:  # asc
                    order_by_clm = (tmp1[1].replace("asc", "")).replace(" ", "")  # tmp1[1] = 'column'
                    asc = True
                if columns == "*":
                    db.select(table, columns, where_condition, order_by_clm, asc, top, into)
                    # Show the table that was saved as...
                    if into is not None:
                        db.show_table(into)
                else:
                    db.select(table, columns.split(","), where_condition, order_by_clm, asc, top, into)
                    if into is not None:
                        db.show_table(into)
        if table == "testtable":
            db.drop_table("testtable")

    elif sql_splitted[0] == "update":  # command update
        update_split = sql.split("update")
        set_split = update_split[1].split("set")
        table = set_split[0].replace(" ", "")
        after_set = set_split[1].replace(" ", "")
        temp = after_set.split("where")
        condition = temp[1].replace(" ", "")
        temp1 = temp[0].split("==")
        column = temp1[0].replace(" ", "")
        value = temp1[1].replace(" ", "")
        db.update(table, value, column, condition)
        db.show_table(table)

    elif sql_splitted[0] == "drop":  # command drop
        drop_split = sql.split("drop")
        table = drop_split[1].replace(" ", "")
        db.drop_table(table)

    else:
        if sql != "exit":
            print("\nWrong syntax!")
