from typing import List
import boto3
import pandas as pd

class GetUsersFromCognitoPool:
    def __init__(self, UserPoolId: str, Region: str, *RequiredAttributes, **file_name) -> None:
        self.UserPoolId = UserPoolId
        self.Region = Region
        self.RequiredAttributes = RequiredAttributes
        self.CsvFileName = "CognitoUsers.csv"
        self.Limit = 60
        self.MAX_NUMBER_RECORDS = 0
        if "file_name" in file_name.keys():
            self.CsvFileName = f"{file_name['file_name']}.CSV"
        self.main()


    def get_list_cognito_users(self, cognito_idp_client, next_pagination_token, limit):
        """_summary_

        Args:
            cognito_idp_client (string): CognitoPool Id
            next_pagination_token (string): Page token for moving next page in CognitoPool Dash Board

        Returns:
            Object: Object of users data
        """
        return (
            cognito_idp_client.list_users(
                UserPoolId=self.UserPoolId, PaginationToken=next_pagination_token, Limit=limit
            )
            if next_pagination_token
            else cognito_idp_client.list_users(
                UserPoolId=self.UserPoolId,
                Limit=limit
            )
        )


    def main(self):
        client = boto3.client("cognito-idp", self.Region)
        self.csv_new_line = {
            self.RequiredAttributes[i]: "" for i in range(len(self.RequiredAttributes))
        }
        try:
            file_storage_local = f"E:/KonfHub/newbackend/cognito_migrations/csv_files/{self.CsvFileName}"
            csv_file = open(file_storage_local, "w")
            csv_file.write(",".join(self.csv_new_line.keys()) + "\n")
        except Exception as error:
            print(error)
            exit()


        pagination_token = ""
        Page_count = 0
        exported_records_counter = 0
        while pagination_token is not None:
            csv_lines = []
            try:
                user_records = self.get_list_cognito_users(
                    cognito_idp_client=client, next_pagination_token=pagination_token, limit = self.Limit if self.Limit < self.MAX_NUMBER_RECORDS else self.MAX_NUMBER_RECORDS
                )
            except client.exceptions.ClientError as err:
                print(err)
                csv_file.close()
                exit()
            except:
                print("ERROR::Something else went wrong")
                csv_file.close()
                exit()


            """ Check if there next paginatioon is exist """
            if set(["PaginationToken", "NextToken"]).intersection(set(user_records)):
                pagination_token = (
                    user_records["PaginationToken"]
                    if "PaginationToken" in user_records
                    else user_records["NextToken"]
                )
                # print("Pagination Token=", pagination_token)
                Page_count += 1
            else:
                pagination_token = None
            

            for user in user_records["Users"]:              # Getting required attributes values and adding into CSV file
                csv_line = self.csv_new_line.copy()
                for required_attribute in self.RequiredAttributes:
                    csv_line[required_attribute] = ""
                    if required_attribute in user.keys():
                        csv_line[required_attribute] = str(user[required_attribute])
                        continue
                    for user_attibute in user["Attributes"]:
                        if user_attibute["Name"] == required_attribute:
                            csv_line[required_attribute] = str(user_attibute["Value"])
                csv_lines.append(",".join(csv_line.values()) + "\n")
            csv_file.writelines(csv_lines)


            exported_records_counter += len(csv_lines)
            print("Page Count:", Page_count)
            print("Number of records exported Count:", exported_records_counter, "\n")
            if pagination_token is None:
                print("End of the Cognito User Pool reached")
                print("INFO::Successful Exported Users Data From Cognito to .CSV file")
                csv_file.close()
            # self.read_data_from_csv(file_storage_local)
            if self.MAX_NUMBER_RECORDS and exported_records_counter >= self.MAX_NUMBER_RECORDS:
                print("INFO: Max Number of Exported Reached")
                break



# Creating Object for Class 
RequiredAttributes = ["name", "Username", "email"]
# file_name={"file_name": "UserData"}
GetUsersFromCognitoPool(
    "ap-southeast-1_5L9AftPMK", "ap-southeast-1", *RequiredAttributes
)
