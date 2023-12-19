import pandas as pd
import json
import click


@click.command()
@click.argument('acl_path')
def main(acl_path):
    df = pd.read_csv(f'{acl_path}/acl_configs.csv')

    acl_list = []

    # Add document link to the future topic configs

    for index, row in df.iterrows():
        acl_id = f"{row['principal']}-{row['resource_name']}-{row['operation']}"
        topic_dict = {
            f"{acl_id}":
                {
                        "resource_type": row['resource_type'],
                        "resource_name": row['resource_name'],
                        "pattern_type": row['pattern_type'],
                        "principal": row['principal'],
                        "host": row['host'],
                        "operation": row['operation'],
                        "permission": row['permission']
                }

            }
        acl_list.append(topic_dict)

    json_output = json.dumps(acl_list, indent=4)

    print(json_output)

    with open(f'{acl_path}/acls.json', 'w') as json_file:
        json_file.write(json_output)


if __name__ == "__main__":
    main()