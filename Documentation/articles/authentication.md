# Authentication and Authorization

## User authentication in ArmoniK

ArmoniK supports user authentication to increase the level of security. It uses **mTLS** to authentify a user with their user certificate.
This authentication is done at 2 levels :
- **Certificate level authentication**: the ingress in front of ArmoniK checks the validity of the user certificate. If validated, it sends the Common Name (CN) and Fingerprint of the certificate as headers to the underlying service
- **Header level authentication**: the received headers are checked against a user database. If the user exists, it is authenticated with the corresponding internal user identity.

Regarding the user identity, and thus permissions, used by the service after authentication, ArmoniK supports the following :
- **Certificate based scheme**: the user identity corresponds to a specific certificate, which CN and Fingerprint are stored in the database.
- **CN based scheme**: the user identity corresponds to the CN of a certificate. If the database contains an entry where only the CN is specified, it will match any valid certificate with this CN, unless it also matches an entry with both CN and Fingerprint in which case the Certificate Based Scheme has priority.
- **Impersonation scheme**: the user identity corresponds to the user chosen via specific headers in the request. This is possible only if the certificate used matches a user, through a Certificate or CN scheme, who has the permissions required to impersonate the chosen user. See the [Impersonation](#impersonation) section for details. 

## User permissions

ArmoniK uses a User-Role-Permission based approach to handle authorization. Each user in the database can have a set of **Roles**. Each role contains a set of **Permissions**. You cannot add permissions directly to a user. A permission is defined as a string in a specific format.
The current version handles the following types of permissions :
|Format|Example|Parameters|Description|
---|---|---|---|
``General:Impersonate:<Rolename>``|``General:Impersonate:Monitoring``|**Rolename**: Name of a role|Grants the right to impersonate a user with the role named \<Rolename\>. See [Impersonation](#impersonation) for details|
|``<Service>:<Name>``|``Submitter:CreateSession``|**Service**: Name of an ArmoniK web service<br>**Name**: Name of the endpoint|Grants the right to use the endpoint named \<Name\> of the service named \<Service\>|
|``<Service>:<Name>:<Target>``|``Submitter:CancelSession:Self``|**Service**: Name of an ArmoniK web service<br>**Name**: Name of the endpoint<br>**Target**: Target or scope of the permission|Same as ``<Service>:<Name>`` as \<Target\> is currently unused|

## Impersonation

ArmoniK allows users to impersonate other users. 