class Cognito:
    client_id = app.config.get('AWS_CLIENT_ID')
    user_pool_id = app.config.get('AWS_USER_POOL_ID')
    identity_pool_id = app.config.get('AWS_IDENTITY_POOL_ID')
    client_secret = app.config.get('AWS_APP_CLIENT_SECRET')
    # Public Keys used to verify tokens returned by Cognito:
    # http://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-tokens-with-identity-providers.html#amazon-cognito-identity-user-pools-using-id-and-access-tokens-in-web-api
    id_token_public_key = app.config.get('JWT_ID_TOKEN_PUB_KEY')
    access_token_public_key = app.config.get('JWT_ACCESS_TOKEN_PUB_KEY')

    def __get_client(self):
        return boto3.client('cognito-idp')

    def get_secret_hash(self, username):
        # A keyed-hash message authentication code (HMAC) calculated using
        # the secret key of a user pool client and username plus the client
        # ID in the message.
        message = username + self.client_id
        dig = hmac.new(self.client_secret..encode('UTF-8'), msg=message.encode('UTF-8'),
                       digestmod=hashlib.sha256).digest()
        return base64.b64encode(dig).decode()

    # REQUIRES that `ADMIN_NO_SRP_AUTH` be enabled on Client App for User Pool
    def login_user(self, username_or_alias, password):
        try:
            return self.__get_client().admin_initiate_auth(
                UserPoolId=self.user_pool_id,
                ClientId=self.client_id,
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters={
                    'USERNAME': username_or_alias,
                    'PASSWORD': password,
                    'SECRET_HASH': self.get_secret_hash(username_or_alias)
                }
            )
        except botocore.exceptions.ClientError as e:
            return e.response
