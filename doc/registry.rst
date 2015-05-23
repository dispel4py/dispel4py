The Registry Interface
======================

The dispel4py framework has been designed with modularity, reusability and collaboration in mind. These requirements are met partly from the abstraction properties of fine-grained workflows in dispel4py and partly from integrating the workflow definition process with a suitable registry of components. A registry reference implementation, in the form of a RESTful Web service, can be downloaded and installed from https://github.com/iaklampanos/dj-vercereg. More information on the rationale and design principles of the registry (without the details necessarily conforming to the current implementation) can be found at http://verce.eu/Repository/Deliverables/RP3/D-JRA2.1.2.pdf. 

The dispel4py Registry Interface is a set of Python methods which allow end-users to interact with the registry in useful ways, should they chose to use a registry for their work. Via the registry interface users can:

* 