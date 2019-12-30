import pandas
import ipywidgets as widgets
import pyvo
from IPython.display import Image, display, clear_output
import networkx as nx
from pyvo.auth import authsession

__all__ = ['QueryBuilder']

class QueryBuilder:
    # Constructor, creating class object button widgets. Call initialize() function.
    # 
    # @param: None
    # @return: None
    def __init__(self):
        self.view_query_button = widgets.Button(
            description="View Query",
            layout=widgets.Layout(width='100px'),
            style=widgets.ButtonStyle(button_color='#C8F7FD'))
        self.view_query_button.on_click(self.__display_query)
        self.clear_button = widgets.Button(
            description="CLEAR",
            layout=widgets.Layout(flex='1 1 auto',
                                  width='auto'),
            style=widgets.ButtonStyle(button_color='#C8F7FD'))
        self.clear_button.on_click(self.__clear_button_clicked)
        self.edit_button = widgets.Button(
            description="EDIT QUERY",
            layout=widgets.Layout(flex='1 1 auto',
                                  width='auto'),
            style=widgets.ButtonStyle(button_color='#C8F7FD'))
        self.edit_button.on_click(self.__edit_button_clicked)
        self.result_query = widgets.Textarea(
                description="",
                value="",
                layout=widgets.Layout(flex='1 1 auto',
                                      width='auto',
                                      height='100%'))
        self.__initialize()
        self.list_test = [self.clear_button, self.edit_button]


    # initialize variables, create output widgets.
    #
    # @param: None
    # @return: None
    def __initialize(self):
        # public class varibale cookie. 
        # assign CADC_SSO cookie to it outside of the query builder class.
        self.cookie = ''
        self.count = 0
        self.count_num_clicks = 0
        self.edit_flag = False        
        self.list_of_join_tables = []
        self.schema_table_dictionary = {}
        self.joinable_dictionary = {}
        self.on_condition_dictionary = {}
        self.column_type_dictionary ={}
        # initialize the graph which will be used to perform dijkstra
        # shortest path finding. 
        self.graph = nx.Graph()
        self.query_out = widgets.Output(
            layout=widgets.Layout(width='100%'))
        self.add_button_output = widgets.Output(
            layout=widgets.Layout(width='100%'))
        self.where_condition_out = widgets.Output(
            layout=widgets.Layout(width='100%'))
        self.query_out.layout.border = "1px solid green"
        self.edit_out = widgets.Output(
            layout=widgets.Layout(width='100%'))
        self.result = widgets.Output(
            layout=widgets.Layout(width='100%'))
        self.out = widgets.Output()
        self.view_query_button.disabled = False


    # public function to start the the query builder.
    #
    # @param: None
    # @return: None
    def Start_query(self):
        with self.out:
            clear_output()
            display(widgets.HBox([self.query_out]))
            self.__get_service()
            display(widgets.HBox(children=self.list_test))
            display(self.edit_out)
            display(self.result)
        display(self.out)


    # Private function to create the service combobox widget, called
    # by start_query function.
    # @param: None
    # @return: None
    def __get_service(self):
        service_combobox_list = [
            'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap/',
            'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/youcat/',
            'https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/']
        self.service_combobox = widgets.Combobox(
            value='https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/argus/',
            options=service_combobox_list,
            description='SERVICE',
            continuous_update=False,
            layout=widgets.Layout(left='-15px',
                                  width='780px'))
        output_schema = widgets.interactive_output(
            self.__get_schema,
            {'service': self.service_combobox})
        display(self.service_combobox)
        display(output_schema)


    # Private function to create the schema dropdown widget.
    # perform three queries:1.get schema name form tap_schema.schema table; 
    # 2.get schema name and table name from tap_schema.tables;
    # 3.get the joinable relationship from tap_schema.keys table join 
    # tap_schema.key_columns table. 
    # The function is interactively called by __get_service() with the service
    # widget.
    # @param: service dropdown widget
    # @return: None
    def __get_schema(self, service):
        try:
            self.joinable_dictionary = {}
            self.on_condition_dictionary = {}
            # if there is a cookie assigned, create a pyvo service with auth.
            if self.cookie != '':
                auth = authsession.AuthSession()
                auth.credentials.set_cookie('CADC_SSO', self.cookie)
                self.service = pyvo.dal.TAPService(service, auth)
            else:
                # else create an anonymous pyvo service.
                self.service = pyvo.dal.TAPService(service)
            table_query1 = "SELECT schema_name FROM tap_schema.schemas"
            table_query2 = """SELECT schema_name, table_name
            FROM tap_schema.tables"""
            table_query3 = """SELECT from_table, target_table, from_column,
            target_column FROM tap_schema.keys JOIN tap_schema.key_columns ON
            tap_schema.keys.key_id=tap_schema.key_columns.key_id"""
            schemas = self.service.search(table_query1)
            tables = self.service.search(table_query2)
            joinables = self.service.search(table_query3)
            tmp = schemas['schema_name']
            schema_list = [x.decode() for x in list(tmp)]
            tmp = tables['schema_name']
            table_schema_list = [x.decode() for x in list(tmp)]
            tmp = tables['table_name']
            table_list = [x.decode() for x in list(tmp)]
            tmp = joinables['from_table']
            from_table_list = [x.decode() for x in list(tmp)]
            tmp = joinables['target_table']
            target_table_list = [x.decode() for x in list(tmp)]
            tmp = joinables['from_column']
            from_column_list = [x.decode() for x in list(tmp)]
            tmp = joinables['target_column']
            target_column_list = [x.decode() for x in list(tmp)]
            for idx in range(0, len(table_schema_list)):
                tmp = table_schema_list[idx]
                # populate schema_table_dictionary, 
                # key:table name, value: schema name
                self.schema_table_dictionary[table_list[idx]] = tmp
            # build the on_condition_dictionary,
            # Example: 
            # {'table_A to table_B': 'table_A.column_1=table_B.column_1',
            #  'table_B to table_A': 'table_B.column_1=table_A.column_1'}
            for idx in range(0, len(from_table_list)):
                f_t = from_table_list[idx]
                t_t = target_table_list[idx]
                f_c = from_column_list[idx]
                t_c = target_column_list[idx]
                r1 = f"{f_t} to {t_t}"
                r2 = f"{t_t} to {f_t}"
                on_condition1 = f"{f_t}.{f_c}={t_t}.{t_c}"
                on_condition2 = f"{t_t}.{t_c}={f_t}.{f_c}"
                if r1 not in self.on_condition_dictionary:
                    self.on_condition_dictionary[r1] = on_condition1
                if r2 not in self.on_condition_dictionary:
                    self.on_condition_dictionary[r2] = on_condition2
            # joinable_dictionary is the graph which be used in the BFS
            for table in table_list:
                self.joinable_dictionary[table] = []
            for idx in range(0, len(from_table_list)):
                f_t = from_table_list[idx]
                t_t = target_table_list[idx]
                if t_t not in self.joinable_dictionary[f_t]:
                    self.joinable_dictionary[f_t].append(t_t)
                    self.joinable_dictionary[t_t].append(f_t)
            for key, value in self.joinable_dictionary.items():
                for value_item in value:
                    self.graph.add_edge(key, value_item)
        except Exception:
            print("Service not found")
            return
        # creating a schema dropdown widget.
        self.schema_dropdown = widgets.Dropdown(
            options=schema_list,
            description='SCHEMA',
            continuous_update=False,
            layout=widgets.Layout(left='-20px',
                                  width='780px'))
        # interactively calling __get_table.
        output_tables = widgets.interactive_output(
            self.__get_table,
            {'schema': self.schema_dropdown})
        # display the widget and the interactive output
        display(self.schema_dropdown)
        display(output_tables)


    # Private function to create table dropdown widget,  
    # The function is interactively called by __get_schema() with
    # the schema widgets
    # @param: schema dropdwon widget
    # @return: None
    def __get_table(self, schema):
        table_list = []
        # if the selected schema is in schema_table_dictionary,
        # add the tables that belong to this schema into table_list.
        for key, value in self.schema_table_dictionary.items():
            if value == schema:
                table_list.append(key)
        # create the first table dropdown widget
        self.table_one = widgets.Dropdown(
            options=table_list,
            description='TABLE',
            layout=widgets.Layout(left='-25px',
                                  width='1050px'))
        # create the first join button widget
        self.join_button = widgets.Button(
            description="ADD",
            icon='',
            layout=widgets.Layout(left='-20px'),
            style=widgets.ButtonStyle(button_color='#C8F7FD'))
        # on_click event listener.
        self.join_button.on_click(self.__add_button_clicked)
        # clear the join tables
        self.list_of_join_tables = []
        # clear the output
        self.add_button_output.clear_output()
        # set the visibility of the join button.
        self.join_button.layout.visibility = 'visible'
        self.view_query_button.click()
        # create a text widget to update the table columns.
        # change this.table_text.value to trigger __get_select_columns()
        # and __set_columns() function.
        self.table_text = widgets.Text(value=self.table_one.value,
                                       description='')
        ouput_columns = widgets.interactive_output(
            self.__get_select_columns,
            {'table_text': self.table_text})
        ouput_where_columns = widgets.interactive_output(
            self.__set_columns,
            {'table_text': self.table_text})
        widgets.interactive_output(
            self.__change_columns,
            {'table': self.table_one})
        # display the widgets and interactive outputs
        display(widgets.HBox([self.table_one, self.join_button]),
                self.add_button_output,
                ouput_columns,
                ouput_where_columns)


    # Private function listens on the button on_click event.
    # When a join button is click, the function creates new table widget 
    # and new join button.
    # @param: button widget
    # @return: None
    def __add_button_clicked(self, b):
        with self.add_button_output:
            # clear the output
            clear_output()
            # if the list_of_join_tables list is empty, store the
            # original(or fist) table and join button in a horizontal
            # Box widget, then add the box into the list_of_join_tables list
            if len(self.list_of_join_tables) < 1:
                self.list_of_join_tables.append(
                    widgets.HBox([self.table_one, self.join_button]))
                # delete all other options for the original table
                self.table_one.options= [self.table_one.value]
                # hide the join button
                self.join_button.layout.visibility = 'hidden'
            # create a new table
            join_table = widgets.Dropdown(
                options=self.__BFS(
                    self.joinable_dictionary,
                    self.list_of_join_tables[-1].children[0].value),
                description='TABLE',
                layout=widgets.Layout(left='-30px',
                                      width='1200px'))
            # create a new join button
            join_button = widgets.Button(
                description="ADD",
                icon='',
                layout=widgets.Layout(left='-25px'),
                style=widgets.ButtonStyle(button_color='#C8F7FD'))
            # the button listens on the same event as the previous join button
            join_button.on_click(self.__add_button_clicked)
            # add the new table and join button into list_of_join_tables list
            self.list_of_join_tables.append(
                widgets.HBox([join_table, join_button]))
            widgets.interactive_output(
                self.__change_columns,
                {'table': join_table})
            # disable all the table options and hide all previous join buttons
            for table in self.list_of_join_tables[1:-1]:
                table.children[0].options= [table.children[0].value]
                table.children[1].layout.visibility = 'hidden'
            # display all the table widgets and hide all the join buttons.
            for x in self.list_of_join_tables[1:]:
                display(x)
            # trigger the view_query_button to update the displayed query.  
            self.view_query_button.click()


    # Private function performs Breadth first search to get the list of tables
    # which have direct and indirect relationship with the selected table.
    # 
    # @param: graph: dictionary, selected_node: table name
    # @return: list of table names
    def __BFS(self, graph, selected_node):
        result = []
        visited = [False] * (len(graph))
        queue = []
        queue.append(selected_node)
        visited[list(graph.keys()).index(selected_node)] = True
        while queue:
            selected_node = queue[0]
            queue = queue[1:]
            result.append(selected_node)
            for i in graph[selected_node]:
                if visited[list(graph.keys()).index(i)] == False:
                    queue.append(i)
                    visited[list(graph.keys()).index(i)] = True
        return result[1:]


    # Private function performs dijkstra shortest path finding algorithm
    # to find the minimum number of tables for a join operation.
    #
    # @param: string start table name, string end table name
    # @return: list of table names
    def __shortest_path(self, start, end):
        return nx.dijkstra_path(self.graph, start, end)


    # Private function triggers button to update
    # interactively called in __get_table function.
    # @param: table_text widget
    # @return: None
    def __set_columns(self, table_text):
        self.tmp_where_condition_dictionary = {}
        # clear the list
        self.list_of_where_object = {}
        self.button_to_trigger = widgets.Button(description = "update")
        self.button_to_trigger.on_click(self.__column_button_clicked)
        # trigger the button
        self.button_to_trigger.click()
        display(self.where_condition_out)


    # Private function to create the fields(except the column field)
    # in WHERE selection.
    #
    # @param: column: dropdown widget, key: text widget
    # @return: None
    def __get_other_fields(self, column, key):
        if self.column_type_dictionary[column] == 'char':
            method_list = ['like', 'equal']
        else:
            method_list = ['>', '<', '>=', '<=', '=']
        self.method = widgets.Dropdown(
            options=method_list,
            description='')
        self.column_value = widgets.Text(
            value='',
            placeholder='value',
            continuous_update=False,
            description='')
        method_ui = widgets.HBox([self.method,
                                  self.column_value],
                                 layout=widgets.Layout(width='100%'))
        # tmp_where_condition_dictionary is used for easy seletion with a key
        self.tmp_where_condition_dictionary[key] = method_ui
        widgets.interactive_output(self.__update_on_value,
                                   {"value": self.column_value})
        display(method_ui)


    # Privaet function to trigger the view_query button to update the
    # displayed query.
    # The function is interactively called in __get_other_fields function
    #
    # @param: value: column dropdown widget
    # @return: None
    def __update_on_value(self, value):
        self.view_query_button.click()


    # Private function listens on the button on_click event.
    # when an '+' or '-' button is triggered, the function will add more
    # WHERE condition object or remove a WHERE condition object.
    #
    # @param: buttob widget
    # @return: None
    def __column_button_clicked(self, b):
        with self.where_condition_out:
            # clear output
            clear_output()
            try:
                columns = self.__get_column_list(self.table_text.value)
                # check if the button is '+' button or a 'update' button
                if (b.description == '+' or b.description == 'update'):
                    description = 'WHERE'
                    # if list_of_where_object is not empty 
                    # create an AND clause 
                    # else create a WHERE clause
                    if len(self.list_of_where_object) != 0:
                        b.description = '-'
                        description = 'AND'
                        b.style = widgets.ButtonStyle(button_color='#C8EAF9')
                    self.create_new_flag = 1
                    column_name = widgets.Dropdown(
                        options=columns,
                        description=description,
                        layout=widgets.Layout(flex='1 1 auto',
                                              width='auto'))
                    # the save_key widget stores current count as string
                    save_key = widgets.Text(value=str(self.count),
                                            description='Key')
                    other_fields = widgets.interactive_output(
                        self.__get_other_fields, {'column': column_name,
                                                  'key': save_key})
                    # create a new '+' button which listens on the same event
                    # as all the previous '+' or '-' buttons. 
                    add_button = widgets.Button(
                        description="+",
                        icon='',
                        tooltip=str(self.count), # set count as tooltip  
                        style=widgets.ButtonStyle(button_color='#C8F7FD'))
                    add_button.on_click(self.__column_button_clicked)
                    # put column_name widget, other_fields widget, and 
                    # add_button widget into a horizontal box.
                    column_box = widgets.HBox([
                        widgets.Box([column_name],
                                    layout=widgets.Layout(width="50%")),
                        widgets.Box([other_fields],
                                    layout=widgets.Layout(top="-6px",
                                                          width="40%")),
                        widgets.Box([add_button],
                                    layout=widgets.Layout(width="10%"))],
                        layout=widgets.Layout(left='-35px'))
                    # check if the list_of_where_object dictionary is empty 
                    # if not, remove other options for the last column name
                    # dropdown widget.
                    if(len(list(self.list_of_where_object.values()))>0):
                        last = list(self.list_of_where_object.values())[-1]
                        where = last.children[0].children[0]
                        where.options = [ where.value] 
                    # insert a new WHERE/AND object into the dictionary with 
                    # the current count as key
                    self.list_of_where_object[str(self.count)] = column_box
                    # increament the count
                    self.count += 1
                # if the button is a '-'  button, remove the last WHERE/AND
                # object from the tmp_where_condition_dictionary dictionary
                # and list_of_where_object dictionary with the tooltip as key
                elif (b.description == '-'):
                    del self.list_of_where_object[b.tooltip]
                    where_obj = list(self.list_of_where_object.values())[0]
                    where_obj.children[0].children[0].description = 'WHERE'
                    del self.tmp_where_condition_dictionary[b.tooltip]
                # display all the widgets
                for key in self.list_of_where_object.keys():
                    display(self.list_of_where_object[key])
                # trigger the view_query button to update the displayed query.    
                self.view_query_button.click()
            # prevent column list not found error from showing
            except Exception:
                pass


    # Private function to update the table_text.value to trigger 
    #  __set_columns function and __get_select_columns.
    # interactively called in __get_table and __add_button_clicked functions.
    # @param: table dropdown widget
    # @return: None
    def __change_columns(self, table):
        if len(self.list_of_join_tables) == 0:
            self.table_text.value = f"(table_name='{table}')"
        else:
            string = ""
            for idx in range(0, len(self.list_of_join_tables)):
                first_join = self.list_of_join_tables[idx].children[0].value
                if idx == 0:
                     string = f"""(table_name='{first_join}'"""
                else:
                    string += f""" OR table_name='{first_join}'"""
            string += ")"
            # update the table_text.value 
            self.table_text.value = string
        # trigger the view_query button to update the displayed query
        self.view_query_button.click()


    # Private function to create the multiple selection widget
    # This function is interactively called in __get_table function
    # @param: table_text: text widget
    # @return: None
    def __get_select_columns(self, table_text):
        columns = self.__get_column_list(table_text)
        self.select_multiple_columns = widgets.SelectMultiple(
                options=columns,
                description='SELECT ',
                disabled=False,
                layout={'left': '-30px', 'width': '770px'})
        #update the query on select_multiple chnage
        widgets.interactive_output(
            self.__update_on_multiple,
            {'select_multiple': self.select_multiple_columns})
        display(widgets.HBox([self.select_multiple_columns]))


    # Private function to trigger view_query button to update query
    # this function is similar to __update_on_value function
    # it's interactively called in __get_select_columns function.
    #
    # @param: select_multiple dropdown widget
    # @return: None
    def __update_on_multiple(self, select_multiple):
        self.view_query_button.click()


    # Private function to get table columns from the database.
    #
    # @param: table_text: string table name
    # @return: list of columns
    def __get_column_list(self, table_text):
        query = f"""SELECT column_name, table_name, indexed, datatype from
        tap_schema.columns WHERE """
        query = query + table_text
        output = self.service.search(query)
        column_lst = [x.decode() for x in list(output['column_name'])]
        table_name = [x.decode() for x in list(output['table_name'])]
        type_lst = [x.decode() for x in list(output['datatype'])]
        indexed_lst = output['indexed']
        # modify column_list list, indicate indexed columns.
        for i in range(0, len(column_lst)):
            if indexed_lst[i] == 1:
                column_lst[i] = f"{table_name[i]}.{column_lst[i]} (indexed) "
            else:
                column_lst[i] = f"{table_name[i]}.{column_lst[i]}"
            self.column_type_dictionary[column_lst[i]] = type_lst[i]
        return column_lst


    # Private function listens on a on_click event,
    # When the edit button is clicked, this function calls __disable_fields to
    # disables/enables all the widgets and displays the
    # result_query widgets.Textarea.
    # 
    # @param: button widget
    # @return: None
    def __edit_button_clicked(self, b):
        with self.edit_out:
            clear_output()
            self.count_num_clicks += 1
            if self.count_num_clicks%2 != 0:
                self.view_query_button.click()
                self.result_query.value = self.query_body
                self.edit_flag = True
                self.__disable_fields(True)
                display(widgets.VBox([self.result_query],
                                     layout=widgets.Layout(height='200px')))
            else:
                self.__disable_fields(False)


    # Private function to disables/enable all the widgets.
    #
    # @param: set_disable: boolean
    # @return: None
    def __disable_fields(self, set_disable):
        self.view_query_button.disabled = set_disable
        self.service_combobox.disabled = set_disable
        self.schema_dropdown.disabled = set_disable
        self.table_one.disabled = set_disable
        self.join_button.disabled = set_disable
        if len(self.list_of_join_tables)> 0:
            for table in self.list_of_join_tables:
                table.children[0].disabled = set_disable
                table.children[1].disabled = set_disable
        self.select_multiple_columns.disabled = set_disable
        for i in list(self.list_of_where_object.values()):
            i.children[0].children[0].disabled = set_disable
            i.children[2].children[0].disabled = set_disable
        for i in list(self.tmp_where_condition_dictionary.values()):
            i.children[0].disabled = set_disable
            i.children[1].disabled = set_disable


    # Public function to run the final query
    #
    # @param: None
    # @return: pyvo table
    def search_query(self):
        self.view_query_button.click()
        if self.edit_flag == True:
            self.edit_flag = False
        else:
            self.result_query.value = self.query_body
        # disable the buttons
        self.edit_button.disabled = True
        self.clear_button.disabled = True
        result = self.service.search(self.result_query.value)
        # enable the buttons
        self.edit_button.disabled = False
        self.clear_button.disabled = False
        return result


    # Private function to wipe out all the output and restart the process.
    #
    # @param: button widget
    # @return: None
    def __clear_button_clicked(self, b):
        self.out.clear_output()
        self.__initialize()
        self.Start_query()


    # Private function to build the assemble the final query and display it.
    #
    # @param: butoon widget
    # @return: None
    def __display_query(self, b):
        with self.query_out:
            clear_output()
            columns = ""
            tables = ""
            wheres = ""
            used_tables = []
            tmp_where_list = []
            # parse the join table clasus.
            if len(self.list_of_join_tables) == 0:
                tables = f"{self.table_one.value}"
            else :
                for index in range(0, len(self.list_of_join_tables)):
                    if index == 0:
                        join_table = self.list_of_join_tables[index]
                        tables = f"{join_table.children[0].value}"
                        used_tables.append(join_table.children[0].value)
                    else:
                        join_table = self.list_of_join_tables[index-1]
                        previous_table = join_table.children[0].value
                        current_table = join_table.children[0].value
                        if current_table not in used_tables:
                            used_tables.append(current_table)
                            join_order = self.__shortest_path(previous_table,
                                                              current_table)
                            for i in range(1, len(join_order)):
                                r = f"{join_order[i-1]} to {join_order[i]}"
                                on_condition = self.on_condition_dictionary[r]
                                tables += "\n" + "JOIN " + join_order[i]
                                tables += " ON " + on_condition
                        else:
                            pass
            # Parse the SELECT clasues.
            if len(self.select_multiple_columns.value) == 0:
                columns = "* \n"
            else:
                for item in self.select_multiple_columns.value:
                    if ' (indexed)' in item:
                        item = item.replace(' (indexed)', '')
                    columns += f"{item}, \n"
                columns = columns[:-3] + ' \n'
            # parse the where clauses.
            for key in self.list_of_where_object.keys():
                tmp_where = self.list_of_where_object[key]
                tmp_condition = self.tmp_where_condition_dictionary[key]
                item1 = tmp_where.children[0].children[0].description
                item2 = tmp_where.children[0].children[0].value
                item3 = tmp_condition.children[0].value
                item4 = tmp_condition.children[1].value
                if item3 == 'like':
                    item4 = f"'%{item4}%'"
                elif item3 == 'equal':
                    item3 = '='
                    item4 = f"'{item4}'"
                if ' (indexed)' in item2:
                    item2 = item2.replace(' (indexed)', '')
                tmp_where_list.append([item1, item2, item3, item4])
            where_length = len(tmp_where_list)
            for index in range(0, where_length):
                w = tmp_where_list[index]
                if where_length == 1:
                    if w[0] == "WHERE" and (w[3] == "" or w[3] == "'%%'"):
                        pass
                    else:
                        wheres += f"{w[0]} {w[1]} {w[2]} {w[3]} \n"
                else:
                    if w[0] == "WHERE" and (w[3] == "" or w[3] == "'%%'"):
                        if index+1 != where_length:
                            tmp_where_list[index+1][0] = "WHERE"
                    elif w[0] == "AND" and (w[3] == "" or w[3] == "'%%'"):
                        pass
                    else:
                        wheres += f"{w[0]} {w[1]} {w[2]} {w[3]} \n"
            # final result query .
            self.query_body = f"SELECT \n{columns}FROM \n{tables} \n{wheres}"
            print(self.query_body)