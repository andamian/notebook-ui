import pandas
import ipywidgets as widgets
import pyvo
from IPython.display import Image, display, clear_output
import networkx as nx
from pyvo.auth import authsession

__all__ = ['QueryBuilder']

class QueryBuilder:
    # 
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


    # initialize variables, create output widgets
    #
    # @param: None
    # @return: None
    def __initialize(self):
        self.cookie = ''
        self.list_of_join_tables = []
        self.count = 0
        self.count_num_clicks = 0
        self.edit_flag = False
        self.schema_table_dictionary = {}
        self.joinable_dictionary = {}
        self.on_condition_dictionary = {}
        self.column_type_dictionary ={}
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


    # 
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


    #
    #
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


    #
    #
    # @param: service dropdown widget
    # @return: None
    def __get_schema(self, service):
        try:
            self.joinable_dictionary = {}
            self.on_condition_dictionary = {}
            if self.cookie != '':
                auth = authsession.AuthSession()
                auth.credentials.set_cookie('CADC_SSO', self.cookie)
                self.service = pyvo.dal.TAPService(service, auth)
                print('logged in with CADC_SSO cookie')
            else:
                self.service = pyvo.dal.TAPService(service)
                print('not logged in')
            table_query1 = "SELECT schema_name FROM tap_schema.schemas"
            table_query2 = """SELECT schema_name, table_name
            FROM tap_schema.tables"""
            table_query3 = """SELECT from_table,target_table,from_column,
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
                self.schema_table_dictionary[table_list[idx]] = tmp
            for idx in range(0,len(from_table_list)):
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
            # joinable_dictionary is the graph which be used in the BFS later
            for table in table_list:
                self.joinable_dictionary[table] = []
            for idx in range(0,len(from_table_list)):
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
        self.schema_dropdown = widgets.Dropdown(
            options=schema_list,
            description='SCHEMA',
            continuous_update=False,
            layout=widgets.Layout(left='-20px',
                                  width='780px'))
        output_tables = widgets.interactive_output(
            self.__get_table,
            {'schema': self.schema_dropdown})
        display(self.schema_dropdown)
        display(output_tables)


    #
    #
    # @param: schema dropdwon widget
    # @return: None
    def __get_table(self, schema):
        table_list = []
        for key, value in self.schema_table_dictionary.items():
            if value == schema:
                table_list.append(key)
        self.table_one = widgets.Dropdown(
            options=table_list,
            description='TABLE',
            layout=widgets.Layout(left='-25px',
                                  width='1050px'))
        self.join_button = widgets.Button(
            description="ADD",
            icon='',
            layout=widgets.Layout(left='-20px'),
            style=widgets.ButtonStyle(button_color='#C8F7FD'))
        self.join_button.on_click(self.__add_button_clicked)
        ## clear the join tables
        self.list_of_join_tables = []
        self.add_button_output.clear_output()
        self.join_button.layout.visibility = 'visible'
        self.view_query_button.click()
        # change this value to trigger columns
        self.table_text = widgets.Text(value=self.table_one.value,
                                       description='')
        ouput_columns = widgets.interactive_output(
            self.__get_select_columns,
            {'table_text':self.table_text})
        ouput_where_columns = widgets.interactive_output(
            self.__set_columns,
            {'table_text':self.table_text})
        widgets.interactive_output(
            self.__change_columns,
            {'table':self.table_one})
        display(widgets.HBox([self.table_one, self.join_button]),
                self.add_button_output,
                ouput_columns,
                ouput_where_columns)


    #
    #
    # @param: button
    # @return: None
    def __add_button_clicked(self, b):
        with self.add_button_output:
            clear_output()
            if len(self.list_of_join_tables) < 1:
                self.list_of_join_tables.append(
                    widgets.HBox([self.table_one, self.join_button]))
                self.table_one.options= [self.table_one.value]
                self.join_button.layout.visibility = 'hidden'
            join_table = widgets.Dropdown(
                options=self.__BFS(
                    self.joinable_dictionary,
                    self.list_of_join_tables[-1].children[0].value),
                description='TABLE',
                layout=widgets.Layout(left='-30px',
                                      width='1200px'))
            join_button = widgets.Button(
                description="ADD",
                icon='',
                layout=widgets.Layout(left='-25px'),
                style=widgets.ButtonStyle(button_color='#C8F7FD'))
            join_button.on_click(self.__add_button_clicked)
            self.list_of_join_tables.append(
                widgets.HBox([join_table, join_button]))
            widgets.interactive_output(
                self.__change_columns,
                {'table':join_table})
            for table in self.list_of_join_tables[1:-1]:
                table.children[0].options= [table.children[0].value]
                table.children[1].layout.visibility = 'hidden'
            for x in self.list_of_join_tables[1:]:
                display(x)
            self.view_query_button.click()


    #
    #
    # @param:
    # @return:
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


    #
    #
    # @param: string start table name, string end table name
    # @return: list of table names
    def __shortest_path(self, start, end):
        return nx.dijkstra_path(self.graph, start, end)


    #
    #
    # @param: table text
    # @return:
    def __set_columns(self, table_text):
        self.tmp_where_condition_dictionary = {}
        # clear the list
        self.list_of_where_object = {}
        self.button_to_trigger = widgets.Button(description = "update")
        self.button_to_trigger.on_click(self.__column_button_clicked)
        # trigger the button
        self.button_to_trigger.click()
        display(self.where_condition_out)


    #
    #
    # @param:
    # @return:
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
        self.tmp_where_condition_dictionary[key] = method_ui
        widgets.interactive_output(self.__update_on_value,
                                   {"value":self.column_value})
        display(method_ui)


    #
    #
    # @param:
    # @return:
    def __update_on_value(self, value):
        self.view_query_button.click()


    #
    #
    # @param:
    # @return:
    def __column_button_clicked(self,b):
        with self.where_condition_out:
            clear_output()
            try:
                columns = self.__get_column_list(self.table_text.value)
                if (b.description == '+' or b.description == 'update'):
                    description = 'WHERE'
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
                    save_key = widgets.Text(value=str(self.count),
                                            description='Key')
                    other_fields = widgets.interactive_output(
                        self.__get_other_fields, {'column': column_name,
                                                  'key':save_key})
                    add_button = widgets.Button(
                        description="+",
                        icon='',
                        tooltip=str(self.count),
                        style=widgets.ButtonStyle(button_color='#C8F7FD'))
                    add_button.on_click(self.__column_button_clicked)
                    column_box = widgets.HBox([
                        widgets.Box([column_name],
                                    layout=widgets.Layout(width="50%")),
                        widgets.Box([other_fields],
                                    layout=widgets.Layout(top="-6px",
                                                          width="40%")),
                        widgets.Box([add_button],
                                    layout=widgets.Layout(width="10%"))],
                        layout=widgets.Layout(left='-35px'))
                    if(len(list(self.list_of_where_object.values()))>0):
                        last = list(self.list_of_where_object.values())[-1]
                        where = last.children[0].children[0]
                        where.options = [ where.value]
                    self.list_of_where_object[str(self.count)] = column_box
                    self.count += 1
                elif (b.description == '-'):
                    del self.list_of_where_object[b.tooltip]
                    where_obj = list(self.list_of_where_object.values())[0]
                    where_obj.children[0].children[0].description = 'WHERE'
                    del self.tmp_where_condition_dictionary[b.tooltip]
                for key in self.list_of_where_object.keys():
                    display(self.list_of_where_object[key])
                self.view_query_button.click()
            # prevent column list not found error from showing
            except Exception:
                pass


    #
    #
    # @param:
    # @return:
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
            self.table_text.value = string
        self.view_query_button.click()


    #
    #
    # @param:
    # @return:
    def __get_where_columns(self, table_text):
        columns = self.__get_column_list(table_text)


    #
    #
    # @param:
    # @return:
    def __get_select_columns(self, table_text):
        columns = self.__get_column_list(table_text)
        self.select_multiple_columns = widgets.SelectMultiple(
                options=columns,
                description='SELECT ',
                disabled=False,
                layout={'left':'-30px','width':'1200px'})
        self.update_query_button = widgets.Button(
            description="UPDATE",
            style=widgets.ButtonStyle(button_color='#C8F7FD'),
            layout=widgets.Layout(left='50px', top='20px'))
        self.update_query_button.layout.visibility = 'hidden'
        self.update_query_button.on_click(self.__update_query_clicked)
        #update the query on select_multiple chnage
        widgets.interactive_output(
            self.__update_on_multiple,
            {'select_multiple':self.select_multiple_columns})
        display(widgets.HBox([self.select_multiple_columns,
                              self.update_query_button]))


    #
    #
    # @param: 
    # @return:
    def __update_on_multiple(self,select_multiple):
        self.view_query_button.click()


    #
    #
    # @param:
    # @return:
    def __get_column_list(self, table_text):
        query = f"""SELECT column_name, table_name, indexed, datatype from
        tap_schema.columns WHERE """
        query = query + table_text
        output = self.service.search(query)
        column_lst = [x.decode() for x in list(output['column_name'])]
        table_name = [x.decode() for x in list(output['table_name'])]
        type_lst = [x.decode() for x in list(output['datatype'])]
        indexed_lst = output['indexed']
        for i in range(0, len(column_lst)):
            if indexed_lst[i] == 1:
                column_lst[i] = f"{table_name[i]}.{column_lst[i]} (indexed) "
            else:
                column_lst[i] = f"{table_name[i]}.{column_lst[i]}"
            self.column_type_dictionary[column_lst[i]] = type_lst[i]
        return column_lst


    #
    #
    # @param:
    # @return:
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


    #
    #
    # @param:
    # @return:
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
        self.update_query_button.disabled = set_disable
        for i in list(self.list_of_where_object.values()):
            i.children[0].children[0].disabled = set_disable
            i.children[2].children[0].disabled = set_disable
        for i in list(self.tmp_where_condition_dictionary.values()):
            i.children[0].disabled = set_disable
            i.children[1].disabled = set_disable


    #
    #
    # @param:
    # @return:
    def search_query(self):
        self.view_query_button.click()
        if self.edit_flag == True:
            self.edit_flag = False
        else:
            self.result_query.value = self.query_body
        self.edit_button.disabled = True
        self.clear_button.disabled = True
        result = self.service.search(self.result_query.value)
        self.edit_button.disabled = False
        self.clear_button.disabled = False
        return result


    #
    #
    # @param:
    # @return:
    def __clear_button_clicked(self, b):
        self.out.clear_output()
        self.__initialize()
        self.Start_query()


    #
    #
    # @param:
    # @return:
    def __update_query_clicked(self, b):
        self.view_query_button.click()


    #
    #
    # @param:
    # @return:
    def __display_query(self, b):
        with self.query_out:
            clear_output()
            columns = ""
            tables = ""
            wheres = ""
            used_tables = []
            tmp_where_list = []
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
            if len(self.select_multiple_columns.value) == 0:
                columns = "* \n"
            else:
                for item in self.select_multiple_columns.value:
                    if ' (indexed)' in item:
                        item = item.replace(' (indexed)', '')
                    columns += f"{item}, \n"
                columns = columns[:-3] + ' \n'
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
            self.query_body = f"SELECT \n{columns}FROM \n{tables} \n{wheres}"
            print(self.query_body)