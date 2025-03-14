# %%
import pandas as pd
import numpy as np


def rename_duplicate_rows(df: pd.DataFrame, column_name:str) -> pd.DataFrame:
    """Renames duplicate row values in a specified column by appending a counter.
    We need this functionality if the user declares that the same node connects
    to two or more upstream targets.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to check for duplicates.

    Returns:
        pd.DataFrame: The DataFrame with renamed row values.
    """
    value_counts = df[column_name].value_counts()
    duplicates = value_counts[value_counts > 1].index.tolist()
    for value in duplicates:
        count = 1
        for index, row in df.iterrows():
            if row[column_name] == value:
                df.loc[index, column_name] = f"{value}_{count}"
                count += 1
    return df


def build_node_x_cords(data_frame: pd.DataFrame, node:str, _x:int = 1, _data:list = None) -> list:
    """Use recurision to determine the X Cordinate, Cluster Count, Node Cluster List, and Branch Order
    of each node. 

    Args:
        data_frame (pd.DataFrame): Dataframe containing the start and end point list.
        node (str): The node you want to want to calculate.
    Returns:
        list: A list of data that can be converted into a dataframe.
    """
    temp_df = data_frame[data_frame['end_point'] == node].reset_index()
    node_cluster_list = "_".join(temp_df['start_point'].to_list())
    target_count = len(temp_df) if len(temp_df) > 0 else 1
    for i, row in temp_df.iterrows():
        next_node = row['start_point']
        node_cords = {'node':next_node, 'x_cord':_x, 'target':node, 'node_cluster_count': target_count, 'node_cluster_list': node_cluster_list, 'branch_order': i+1}
        if _data:
            _data.append(node_cords)
        else:
            _data = [node_cords]
        build_node_x_cords(data_frame=data_frame, node=next_node, _x=_x+1, _data=_data)
    return _data


def get_max_nodes_in_branch(data_frame: pd.DataFrame, node:str) -> int:
    """For any given node, find out how the max number of nodes in all
    sub branches.

    Args:
        data_frame (pd.DataFrame): Dataframe containing the start and end point list.
        node (str): The node you want to want to calculate.

    Returns:
        int: Max number of nodes within all downstream sub-branches.
    """
    temp_df = data_frame[data_frame['end_point'] == node].reset_index()
    total_count = len(temp_df) if len(temp_df) > 0 else 1
    down_stream_cnt = 0
    for i, row in temp_df.iterrows():
        next_node = row['start_point']
        max_down_nodes = get_max_nodes_in_branch(data_frame=data_frame, node=next_node)
        down_stream_cnt += max_down_nodes
    return total_count if total_count > down_stream_cnt else down_stream_cnt
    

def assign_y_values(data_frame:pd.DataFrame, start_point:str, min_range:int, max_range: int, y_values_list: list = None) -> list:
    """Assign the y value to each node using the range available for that node.

    Args:
        data_frame (pd.DataFrame): _description_
        start_point (str): _description_
        min_range (int): _description_
        max_range (int): _description_
        y_values_list (list, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    # Now we need to find the position of y..
    # start at a.. and define upper and lower as 8, 1...
    # the Y value of A should be 4.5 (in the exact middle of the bounds.. .5 when even num.. whole num when odd)
    # Next we find that A has two targets.. b and c
    # b has 5 down clusters and c has 3
    # we assign 1-5 to b and all of its downstream
    # and assign 6-8 to c accordingling
    # once we go to b.. it has d,e,f
    # d has 3 max downsources.. so it gets 1-3.. e has 1 so 4, f has 1 so 5
    # when we go down d... d and j are single... so should land on 2
    # l, m, n occupy 1,2,3
    # How we can use a list to assign items..
    # the below can be turned into a loop to continue
    # to loop until all needed items are assigned
    y_val = assign_y_from_range(min_range=min_range, max_range=max_range)
    if not y_values_list:
        y_values_list = []
    y_values_list.append({'node':start_point, 'y_cord':float(y_val)})
    temp_df = data_frame[data_frame['target'] == start_point].reset_index()
    assignable_range = list(range(min_range, max_range+1))
    for _, row in temp_df.iterrows():
        needed_slots = row['max_downstream_cluster_size']
        use_range = assignable_range[:needed_slots]
        new_min_range = min(use_range)
        new_max_range = max(use_range)
        assignable_range = assignable_range[needed_slots:]
        next_start_point = row['node']
        assign_y_values(data_frame=data_frame, start_point=next_start_point, min_range=new_min_range, max_range=new_max_range, y_values_list=y_values_list)
    return y_values_list
    

def assign_y_from_range(min_range:int, max_range:int) -> float:
    """Assign the Y cordinate by adding the branch minimum and
    maximum range values then divide by 2.

    Args:
        min_range (int): Minimum value of the branch range.
        max_range (int): Maximum value of the branch range.

    Returns:
        float: The Y cordinate of the node.
    """
    return (max_range+min_range)/2


def start_hierarchy_dataframe(node_edge_df: pd.DataFrame, node:str) -> pd.DataFrame:
    node_data = build_node_x_cords(data_frame=node_edge_df, node=original_node)
    node_data.append({'node':node, 'x_cord':0, 'y_cord':0, 'target':'None', 'node_cluster_count': 1, 'node_cluster_list': node, 'branch_order': 1})
    df = pd.DataFrame(node_data)
    df['original_node'] = node
    df.sort_values(by=['x_cord', 'y_cord', 'target'], inplace=True)
    return df


def build_cluster_dataframe(hierarchy_dataframe: pd.DataFrame) -> pd.DataFrame:
    cluster_df = hierarchy_dataframe[['node_cluster_list', 'target', 'node', 'node_cluster_count']]
    cluster_df.drop_duplicates(inplace=True, ignore_index=True)
    next_cluster_df = cluster_df.copy().reset_index(drop=True)
    next_cluster_df = next_cluster_df[['node_cluster_list', 'target', 'node_cluster_count']]
    next_cluster_df.rename(columns={
        'node_cluster_count': 'next_cluster_count',
        'node_cluster_list': 'next_cluster_list',
        'target': 'node'
    }, inplace=True)
    next_cluster_df.drop_duplicates(inplace=True, ignore_index=True)

    prior_cluster_df = cluster_df.copy().reset_index(drop=True)
    prior_cluster_df = prior_cluster_df[['node_cluster_list', 'node_cluster_count', 'node']]
    prior_cluster_df.rename(columns={
        'node_cluster_count': 'prior_cluster_count',
        'node_cluster_list': 'prior_cluster_list',
        'node': 'target'
    }, inplace=True)
    prior_cluster_df.drop_duplicates(inplace=True, ignore_index=True)
    cluster_df = pd.merge(hierarchy_dataframe, next_cluster_df, how='left', on='node')
    cluster_df = pd.merge(cluster_df, prior_cluster_df, how='left', on='target')
    return cluster_df


def build_downstream_nodes_dataframe(start_node:str) -> pd.DataFrame:
    down_stream_max_nodes_cnt = []
    for i, row in df.iterrows():
        start_point = row['node']
        check_max = get_max_nodes_in_branch(node_edge_df, start_point)
        down_stream_max_nodes_cnt.append({'node': start_point, 'max_downstream_cluster_size': check_max})
    max_nodes_df = pd.DataFrame(down_stream_max_nodes_cnt)
    return max_nodes_df


def optimize_data_for_tableau(hierarchy_dataframe: pd.DataFrame) -> pd.DataFrame:
    cords_df = hierarchy_dataframe[['node', 'x_cord', 'y_cord']]
    cords_df.rename(columns={
        'node': 'target',
        'x_cord': 'target_x_cord',
        'y_cord': 'target_y_cord'
    }, inplace=True)
    temp_df = pd.merge(hierarchy_dataframe, cords_df, how='left', on='target')
    temp_df = temp_df [[
        'original_node', 'node', 'target', 'x_cord', 'y_cord',
        'target_x_cord', 'target_y_cord', 'branch_order', 'node_cluster_count',
        'next_cluster_count', 'prior_cluster_count', 'max_downstream_cluster_size',
        'overall_max_cluster_size'
    ]]
    target_df = temp_df [[
        'original_node', 'node', 'target',
        'target_x_cord', 'target_y_cord', 'branch_order', 'node_cluster_count',
        'next_cluster_count', 'prior_cluster_count', 'max_downstream_cluster_size',
        'overall_max_cluster_size'
    ]]
    target_df.rename(columns={
        'target_x_cord': 'x_cord',
        'target_y_cord': 'y_cord'
    }, inplace=True)
    target_df['node_type'] = 'target'
    target_df = target_df[~target_df['x_cord'].isnull()]
    source_df = temp_df [[
        'original_node', 'node', 'target', 'x_cord', 'y_cord','branch_order', 'node_cluster_count',
        'next_cluster_count', 'prior_cluster_count', 'max_downstream_cluster_size',
        'overall_max_cluster_size'
    ]]
    source_df['node_type'] = 'source'
    return pd.concat([target_df, source_df])


file_path = f'C:/Users/Kyle/downloads/node_edge.csv'
file_path = f'C:/Users/Kyle/downloads/node_edge_test_3.csv'

start_point_identifier = np.nan

node_edge_df = pd.read_csv(file_path).drop_duplicates(ignore_index=True)
node_edge_df = rename_duplicate_rows(df=node_edge_df, column_name='start_point')

node_edge_df = node_edge_df[['start_point', 'end_point']]
start_points_df = node_edge_df[node_edge_df['end_point'].isnull()]

for i, sp_row in start_points_df.iterrows():
    original_node = sp_row['start_point']
    df = start_hierarchy_dataframe(node_edge_df=node_edge_df, node=original_node)
    start_point = original_node
    #prior cluster count
    df = build_cluster_dataframe(hierarchy_dataframe=df)
    # Join target on node to get the prior cluster info
    # for each node (start point).. what is the max number of nodes within
    # the same branch...
    down_stream_max_nodes_cnt = []
    for i, row in df.iterrows():
        start_point = row['node']
        check_max = get_max_nodes_in_branch(node_edge_df, start_point)
        down_stream_max_nodes_cnt.append({'node': start_point, 'max_downstream_cluster_size': check_max})
    max_nodes_df = pd.DataFrame(down_stream_max_nodes_cnt)
    df = pd.merge(df, max_nodes_df, how='left', on='node')
    overall_max_cluster_size = df['max_downstream_cluster_size'].max()
    overall_min_cluster_size = df['max_downstream_cluster_size'].min()
    # These define the upper and lower bounds of the graph
    df['overall_max_cluster_size'] = overall_max_cluster_size
    df['overall_min_cluster_size'] = overall_min_cluster_size
    
    y_values = assign_y_values(data_frame=df, start_point=original_node, min_range=overall_min_cluster_size, max_range=overall_max_cluster_size)
    y_values_df = pd.DataFrame(y_values)
    # I would keep doing this for all targets/run out of range
    del df['y_cord']
    df = pd.merge(df, y_values_df, how='left', on='node')
    df = optimize_data_for_tableau(hierarchy_dataframe=df)
    df.to_excel(f'c:/users/kyle/downloads/original_node_{original_node}_data_set.xlsx')

# %%
