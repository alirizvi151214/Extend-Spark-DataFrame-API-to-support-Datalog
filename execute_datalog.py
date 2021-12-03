from pyspark.sql import SparkSession
import datalog_ds
from pyspark.sql.functions import col
import copy


def get_atom_df_mapping(df_list, each_atom_args):

    """
    Method for identifying datalog arguments and dataframe columns.
    Also separating out constants for applying filter on data frame
    :param df_list: Dataframe Columns list
    :param each_atom_args: Datalog Atom arguments
    :return: Two dictionaries - Variables and Constants
    """

    map_dict = dict()
    cons_filter_dict = dict()
    for each_index in range(0, len(df_list)):
        if type(each_atom_args[each_index]) == datalog_ds.DLVariable:
            map_dict[each_atom_args[each_index]] = str(df_list[each_index])
        else:
            cons_filter_dict[df_list[each_index]] = str(each_atom_args[each_index])

    return map_dict, cons_filter_dict


def filter_atom_df(df, cons_dict):

    """
    MEthod for filtering dataframe
    :param df: Dataframe
    :param cons_dict: Constant value
    :return: Updated dataframe
    """

    for k, v in cons_dict.items():
        df = df.filter((col(k) == v))
    return df


def process_datalog_rules(mapping_dict, datalog_rule):

    """
    Method for processing each datalog rule atom to transform data frame dynamically
    :param mapping_dict: Datalog Rule predicate name and dataframe mapping dictionary
    :param datalog_rule: Specific datalog rule of given datalog program
    :return: Transformed single dataframe for given datalog rule
    """

    all_col_atom_dict = dict()
    transformed_df_list = []
    for each_atom in datalog_rule.body:
        predicate = each_atom.predicate_name
        if predicate in mapping_dict:
            atom_df = mapping_dict[predicate]

            df_col_list = atom_df.columns
            each_atom_args = each_atom.args_list

            col_atom_dict, cons_filter_dict = get_atom_df_mapping(df_col_list, each_atom_args)

            if not all_col_atom_dict:
                all_col_atom_dict.update(col_atom_dict)
            else:
                for k, v in col_atom_dict.items():
                    all_col_atom_dict = update_dict_with_list_obj(all_col_atom_dict, k, v)

            if cons_filter_dict:
                atom_df = filter_atom_df(atom_df, cons_filter_dict)

            transformed_df_list.append(atom_df)

    # already have last df, so removing from list
    transformed_df_list.pop()
    # traversing from last to first one by one to perform join operation
    inner_join_counter = 0
    for each_df in reversed(transformed_df_list):
        for k, v in all_col_atom_dict.items():
            if isinstance(v, list):
                cur_val = v.pop()
                for each_col in reversed(v):
                    # handle atom df, as having same name of cols when joining
                    atom_df = atom_df.join(each_df, atom_df[cur_val] == each_df[each_col])
                    inner_join_counter += 1
                    break
        if inner_join_counter == 0:
            atom_df = atom_df.join(each_df)

    # convert remaining dict key list to string
    for i, j in all_col_atom_dict.items():
        if isinstance(j, list):
            all_col_atom_dict[i] = "".join(j)

    atom_df.show()
    print(all_col_atom_dict)

    print(datalog_rule.head.args_list)
    select_col_list = []
    for each_head_arg in datalog_rule.head.args_list:
        # print(all_col_atom_dict['X'])
        select_col_list.append(all_col_atom_dict.get(each_head_arg))

    print(select_col_list)
    atom_df = atom_df.select(select_col_list)

    # Showing result
    atom_df.show()
    return atom_df


def merge_same_dl_rules(dl_program_dict):

    """
    Method for implementing concept of datalog of performing
    union if we find two same predicate names
    :param dl_program_dict: Dictionary where two dataframes are updated for union operation
    :return: Updated dictionary like each key has single dataframe
    """

    for k, v in dl_program_dict.items():
        if isinstance(v, list):
            final_df = v[0]
            for each_df in v[1:]:
                final_df = final_df.union(each_df)

            dl_program_dict[k] = final_df

    return dl_program_dict


def update_dict_with_list_obj(dict_obj, key, value):

    """
    Method for adding dataframes in common dictionary keys for further processing
    :param dict_obj: Dictionary which needs to be updated
    :param key: Dictionary key for which value will be added as list
    :param value: Setting value in dictionary key
    :return: Updated Dictionary for the corresponding key
    """

    if key in dict_obj:
        tmp_list = []
        tmp_value = dict_obj[key]
        if isinstance(tmp_value, list):
            df_tmp_list = tmp_list + tmp_value
        else:
            tmp_list.append(tmp_value)
        tmp_list.append(value)
        dict_obj[key] = tmp_list
    else:
        dict_obj[key] = value

    return dict_obj


def get_datalog_result(df_var_mapping_dict, dl_program, user_choice=None):

    """
    Method for analyzing each rule and transforming
    the corresponding data fame of given mapping dictionary
    :param df_var_mapping_dict: Datalog Rule predicate name and dataframe mapping dictionary
    :param dl_program: Entire datalog query
    :param user_choice: User input - what he wants to get back as output
    :return: Can be MAP, STRING, DATAFRAME (as per user input)
    """

    dl_tmp_dict = dict()
    for each_obj_rule in dl_program.dlv_rule:
        each_rule_predicate = each_obj_rule.head.predicate_name

        # method to process each rule in datalog program
        each_obj_rule_df = process_datalog_rules(df_var_mapping_dict, each_obj_rule)

        # Preparation for preforming union operation in the form of dictionary
        dl_tmp_dict = update_dict_with_list_obj(dl_tmp_dict, each_rule_predicate, each_obj_rule_df)
        print("dl_tmp_dict:", dl_tmp_dict)

    # Merge/Union of same predicate name datalog rules
    dl_program_results_map = merge_same_dl_rules(dl_tmp_dict)

    if user_choice is None:
        return dl_program_results_map
    else:
        if user_choice in dl_program_results_map:
            return dl_program_results_map[user_choice]
        else:
            return "Please provide correct input"


def get_first_demo_program():

    # reading data frames
    df1 = spark.read.csv('datasets/students.csv', header=True)

    # declaring Datalog Variables
    p = datalog_ds.DLVariable("P")
    q = datalog_ds.DLVariable("Q")
    r = datalog_ds.DLVariable("R")
    s = datalog_ds.DLVariable("S")

    # Building Datalog Head
    dl_head_demo1 = datalog_ds.DLAtom("Q", [p, q])

    # declaring Datalog Body
    dl_body_demo1 = datalog_ds.DLAtom("Student", [p, q, r, s])

    obj_rule_demo1 = datalog_ds.DLRule(dl_head_demo1)
    obj_rule_demo1.add_body(dl_body_demo1)
    print("first demo rule:", obj_rule_demo1)

    obj_program_demo1 = datalog_ds.DLProgram()
    obj_program_demo1.add_rule(obj_rule_demo1)

    print("first demo program:", obj_program_demo1)

    df_mapping_demo1_dict = {"Student": df1}

    return df_mapping_demo1_dict, obj_program_demo1


def get_second_demo_program():

    # reading data frames
    df1 = spark.read.csv('datasets/students.csv', header=True)
    df2 = spark.read.csv('datasets/student_programs.csv', header=True)

    # declaring Datalog Variables
    p = datalog_ds.DLVariable("P")
    q = datalog_ds.DLVariable("Q")
    r = datalog_ds.DLVariable("R")
    s = datalog_ds.DLVariable("S")
    t = datalog_ds.DLVariable("T")

    # Building Datalog Head
    dl_head_demo2 = datalog_ds.DLAtom("Student", [q, t])

    # declaring Datalog Body
    dl_body_demo2 = datalog_ds.DLAtom("Student", [p, q, r, s])
    dl_body1_demo2 = datalog_ds.DLAtom("Takes", [p, t])

    obj_rule_demo2 = datalog_ds.DLRule(dl_head_demo2)

    obj_rule_demo2.add_body(dl_body_demo2)
    obj_rule_demo2.add_body(dl_body1_demo2)
    print("second demo rule:", obj_rule_demo2)

    obj_program_demo2 = datalog_ds.DLProgram()
    obj_program_demo2.add_rule(obj_rule_demo2)
    print("second demo program:", obj_program_demo2)

    df_mapping_demo2_dict = {"Student": df1, 'Takes': df2}

    return df_mapping_demo2_dict, obj_program_demo2


def get_third_demo_program():

    # reading data frames
    df1 = spark.read.csv('datasets/students.csv', header=True)
    df2 = spark.read.csv('datasets/student_programs.csv', header=True)

    # declaring Datalog Variables
    p = datalog_ds.DLVariable("P")
    q = datalog_ds.DLVariable("Q")
    r = datalog_ds.DLVariable("R")
    s = datalog_ds.DLVariable("S")

    # declaring Datalog Constants
    cs595_c = datalog_ds.DLConstants("cs595")

    # Building Datalog Head
    dl_head_demo3 = datalog_ds.DLAtom("BigDataStudents", [q])

    # declaring Datalog Body
    dl_body_demo3 = datalog_ds.DLAtom("Student", [p, q, r, s])
    dl_body1_demo3 = datalog_ds.DLAtom("Takes", [p, cs595_c])

    obj_rule_demo3 = datalog_ds.DLRule(dl_head_demo3)

    obj_rule_demo3.add_body(dl_body_demo3)
    obj_rule_demo3.add_body(dl_body1_demo3)
    print("third demo rule:", obj_rule_demo3)

    obj_program_demo3 = datalog_ds.DLProgram()
    obj_program_demo3.add_rule(obj_rule_demo3)
    print("third demo program:", obj_program_demo3)

    df_mapping_demo3_dict = {"Student": df1, 'Takes': df2}

    return df_mapping_demo3_dict, obj_program_demo3


def get_forth_demo_program():

    # reading data frames
    df1 = spark.read.csv('datasets/students.csv', header=True)
    df2 = spark.read.csv('datasets/student_programs.csv', header=True)
    df3 = spark.read.csv('datasets/student_interest.csv', header=True)

    # declaring Datalog Variables
    p = datalog_ds.DLVariable("P")
    q = datalog_ds.DLVariable("Q")
    r = datalog_ds.DLVariable("R")
    s = datalog_ds.DLVariable("S")

    # declaring Datalog Constants
    cs595_c = datalog_ds.DLConstants("cs595")
    badminton_c = datalog_ds.DLConstants("badminton")

    # Building Datalog Head
    dl_head_demo4 = datalog_ds.DLAtom("BigdataBadmintonStudents", [q])

    # declaring Datalog Body
    dl_body_demo4 = datalog_ds.DLAtom("Student", [p, q, r, s])
    dl_body1_demo4 = datalog_ds.DLAtom("Takes", [p, cs595_c])
    dl_body2_demo4 = datalog_ds.DLAtom("Interest", [p, badminton_c])

    # Building Datalog Body
    obj_rule_demo4 = datalog_ds.DLRule(dl_head_demo4)
    obj_rule1_demo4 = copy.deepcopy(obj_rule_demo4)
    obj_rule_demo4.add_body(dl_body_demo4)
    obj_rule_demo4.add_body(dl_body1_demo4)
    print("forth demo rule 1:", obj_rule_demo4)

    obj_rule1_demo4.add_body(dl_body_demo4)
    obj_rule1_demo4.add_body(dl_body2_demo4)
    print("forth demo rule 2:", obj_rule1_demo4)

    obj_program_demo4 = datalog_ds.DLProgram()
    obj_program_demo4.add_rule(obj_rule_demo4)
    obj_program_demo4.add_rule(obj_rule1_demo4)
    print("forth demo program:", obj_program_demo4)

    df_mapping_demo4_dict = {'Student': df1, 'Takes': df2, 'Interest': df3}

    return df_mapping_demo4_dict, obj_program_demo4

# This is the entry point of the project
if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    
    # df_mapping_dict, obj_program = get_first_demo_program()
    # # Calling main logic functions for transformations
    # result_df = get_datalog_result(df_mapping_dict, obj_program, "Q")
    #
    # df_mapping_dict, obj_program = get_second_demo_program()
    # # Calling main logic functions for transformations
    # result_df = get_datalog_result(df_mapping_dict, obj_program, "Student")
    #
    # df_mapping_dict, obj_program = get_third_demo_program()
    # # Calling main logic functions for transformations
    # result_df = get_datalog_result(df_mapping_dict, obj_program, "BigDataStudents")

    df_mapping_dict, obj_program = get_forth_demo_program()
    # Calling main logic functions for transformations
    result_df = get_datalog_result(df_mapping_dict, obj_program, "BigdataBadmintonStudents")

    print("final result:")
    if isinstance(result_df, str):
        print(result_df)
    else:
        result_df.show()
