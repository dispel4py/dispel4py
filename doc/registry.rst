The dispel4py Registry Interface
********************************

The dispel4py framework has been designed with modularity, reusability and collaboration in mind. These requirements are met partly from the abstraction properties of fine-grained workflows in dispel4py and partly from integrating the workflow definition process with a suitable registry of components. A registry reference implementation, in the form of a RESTful Web service, can be downloaded and installed from https://github.com/iaklampanos/dj-vercereg. The registry reference implementation is a standard Python Django 1.7 + MySQL product. More information on the rationale and design principles of the registry (without the details necessarily conforming to the current implementation) can be found at http://verce.eu/Repository/Deliverables/RP3/D-JRA2.1.2.pdf. 

The dispel4py Registry Interface is a set of Python methods which allow end-users to interact with the registry in useful ways, should they choose to use a registry for their work. Via the registry interface users can:

* Create workspaces either from scratch or by cloning existing ones
* Copy items across workspaces
* Search for and list items of workspaces
* Search for and list workspaces
* Register PEs, functions and literals
* Delete PEs, functions and literals
* View the details of registered entities

Downloading and Installation
============================

As the dispel4py registry interface is ongoing work it is not in the main dispel4py branch and as such, it is not available via ``pip`` or similar installers.

After having ``git clone``'d dispel4py, users will find it in the ``vercereg`` branch::

    $git clone https://github.com/dispel4py/dispel4py.git
    $git checkout vercereg

Please note that in order for dispel4py to be usable this way, its directory must be added to the PYTHONPATH environment variable::

    $export PYTHONPATH=<PATH TO DISPEL4PY>:$PYTHONPATH

The registry interface can then be found at the path ``<DISPEL4PY_HOME>/dispel4py/registry``.

Configuration and Execution
===========================

The dispel4py registry interface can be configured either via a configuration script or appropriate environment variables, with the first option taking precedence over the second. The recommended option is the use of the configuration file both for convenience as well as for security reasons.

Registry interface configuration is placed in ``~/.d4p_reg.json`` and is in the json format. A configuration file will look like the following::

    {
        "URL": "http://registry.server.location:port",
        "username": "a_user",
        "password": "a_password",
        "workspace": "default_workspace"
    }

Correspondingly, the environment variables recognised by the registry interface are ``D4P_REG_URL``, ``D4P_REG_USERNAME``, ``D4P_REG_PASSWORD`` and ``D4P_REG_WORKSPACE`` and can be set in the same way as the configuration file.

The ``workspace`` configuration (``D4P_REG_WORKSPACE`` environment variable) will point to a workspace owned by the logged in user, which will be the default active workspace of a registry interface session. As users are always expected to find a ``root`` workspace available, ``root`` should be the default workspace name used.

Starting the registry interface
-------------------------------

Once the dispel4py registry interface has been configured, it can be run inside a Python interpreter via executing::

    import dispel4py.registry.registry as r

Alternatively, when in the registry interface's directory (``<dispel4py home>/dispel4py/registry``), users can execute the ``iregclient.sh`` or the ``regclient.sh`` script, which will start the interface inside an `IPython <http://ipython.org>`_ or the standard Python shell respectively::

    $./iregclient.sh

Both alternatives expose the interface via the convenience name ``r``. Interface commands can then be executed by issuing::

    >>> r.<command>

dispel4py Registry Interface Usage and Commands
===============================================
The dispel4py registry interface offers basic interaction with the dispel4py registry in the form of a few simple commands. After the registry interface has been started, users can find out about all the available commands and their syntax details by issuing the ``help`` command::

    >>> r.help()
    
    Commands:
        help([command]): Outputs help for the dispel4py registry's interactive interface.
        info(): Outputs information about the current session.
        (...)

Workspaces
----------
The dispel4py registry has been designed around the notion of *workspaces*. A workspace is an isolated area which contains workflow elements, such as processing elements (PEs) and functions. In dispel4py, PEs are objects of a specific class/type, while functions are arbitrary Python functions. A dispel4py workflow, *i.e.* its topology, is typically described as a function. The registry does not currently distinguish between workflows and arbitrary Python functions.

Most operations exposed by the dispel4py registry assume that a workspace has been selected as being the currently active one and are executed against it. The command `info` provides users with basic information, including which workspace is the currently active one::

    >>> r.info()
    [Current workspace]
    (https://registry.server/workspaces/1/) root - admin
    [Default workspace]
    (https://registry.server/workspaces/1/) root - admin
    [Registry endpoint]
    https://registry.server
    [Registry user]
    admin
	

Users can create a new workspace either by cloning an existing one, *e.g.*::

    >>> r.clone('Sample_Workspace', 'Sample description.')

or by creating a new, empty one::

    >>> r.mk_workspace('Empty_Workspace', 'Empty workspace description')

In the case of cloning, the contents of the source workspace are deep-cloned in the new workspace, while in the second case the new workspace contains nothing.

Listing Workspace Elements
``````````````````````````
The command ``wls`` is used to list the contents of the currently active workspace::

    >>> r.wls()
    [PEs]
    (https://registry.server/pes/1) pes.str.test.StringConcatenator
    [PE Implementations]
    (https://registry.server/peimpls/1) pes.str.test._impls.StringConcatenator
    [Functions]
    (https://registry.server/functions/1) testfns.myfn6
    [Function Implementations]
    (https://registry.server/fnimpls/1) testfns._impls.myfn6
    [Literals]
    (https://registry.server/literals/1) literals.pi
    (https://registry.server/literals/2) my_package.name
    [Packages]
    literals
    my_package
    pes.str.test
    pes.str.test._impls
    testfns
    testfns._impls

Viewing Workspace Elements
``````````````````````````
More detailed inspection of registered elements can be achieved by the ``view`` command::

    >>> r.view('pes.str.test.StringConcatenator')
    Name: pes.str.test.StringConcatenator
    URL: https://registry.server/pes/1/
    Description: This PE concatenates the strin [...]
    Implementations:
    (https://registry.server/peimpls/1/) pes.str.test._impls.StringConcatenator

Deleting Workspace Elements
```````````````````````````
Registered elements be removed by the ``rm`` command::

    >>> r.rm('my_package.name')
    Deleted my_package.name (https://registry.server/literals/2/)
	

Searching for Workspaces and Workspace Elements
```````````````````````````````````````````````

The registry interface provides functions for searching for workspaces based on their name and description::

    >>> r.find_workspaces('my search string')
    (https://registry.server/workspaces/1/) root: The root workspace. It contain [...]
    Total: 1

If the search string is left blank, a full listing of available workspaces is provided.

Users of the registry interface can also search within the currently active workspace::

    >>> r.find_in_workspace('concatenate')
    (https://registry.server/pes/1/) pes.str.test.StringConcatenator: This PE concatenates the strin [...]
    Total: 1


Switching Workspaces
````````````````````
At any given time the registry interface maintains a currently active workspace, which is the default workspace within operations take place. Users can switch the active workspace by name and, if the workspace to switch to is not owned by the current user, owner username::

    >>> r.set_workspace('root', 'admin')
    Default workspaces set to: root (admin) [1]


Copying Workspace Elements
``````````````````````````
Apart from cloning, users can copy individual registry components to a different workspace. In the example below, the user ``bob`` sets the default workspace to be the ``root`` workspace, he creates a new empty workspace and  copies a literal from the root workspace over to the new workspace::

    >>> r.set_workspace('root', 'admin')
    Default workspaces set to: root (admin) [1]
    >>> r.mk_workspace('bob_wspc', 'my sample workspace')
    New workspace created: https://registry.server/workspaces/10/
    
    >>> r.find_workspaces()
    (https://registry.server/workspaces/1/) root: The root workspace. It contain [...]
    (https://registry.server/workspaces/10/) bob_wspc: my sample workspace
    Total: 2
    
    >>> r.copy('literals.pi', 'bob_wspc')
    Created literals.pi in workspace bob_wspc (bob)
    
    >>> r.set_workspace('bob_wspc')
    Default workspace set to: bob_wspc (bob) [10]
    >>> r.wls()
    [Literals]
    (https://registry.server/literals/5) literals.pi
    [Packages]
    literals
    >>> r.view('literals.pi')
    Name: literals.pi
    URL: https://registry.server/literals/5/
    Description: The pi constant
    Origin: https://registry.server/literals/4/
    Value: 3.1415926535897932384626433832795028841971693


Registering and Importing
=========================
One of the main purposes of the registry is to provide users with suitable registration functions for their dispel4py programs and workflow components, and subsequently allow for transparent use within their dispel4py as well as other Python-based workflows. Once registered, all registered objects can be imported in a dispel4py workflow, or generally a Python program, provided the registry interface has been configured appropriately, as described above, and imported in the program::

    import dispel4py.registry.registry
    
    # The following will now fetch the corresponding object from the registry
    from pes.str.test.StringConcatenator import StringConcatenator
	
Registration of components via the registry interface is achieved via the ``register_pe``, ``register_fn`` and ``register_literal`` commands. PEs, functions and literals are registered based on the metadata they carry. In the case of PEs and functions, due to their more complicated metadata, the interface relies on Python docstrings, which are expected to conform to a specific format, outlined below. For the case of literals, due to the current Registry implementation, they can be registered directly from the interface by passing a package, a name and a value.

Registering functions
---------------------
Arbitrary Python functions can be imported via the ``register_fn`` command::

    >>> r.register_fn('tests.randfn.next_rand')
    Registered function: https://registry.server/functions/2/

The successful registration of a function is based on the docstring of the function to be registered. The argument passed to the ``register_fn`` command is a string with the name of the module to be registered. In order for the interface to be able to parse and register the function, the name given must be importable by Python, *i.e.* in the example above the command ``import tests.randfn.next_rand`` must return with no errors.

Now, let's take a look at the file containing the function to import::

    import random
    
    def next_rand(beg, end):
        """
        Return a random integer from beg to end inclusive.
        :name fns.rand
        :param beg: <int> the minimum number to choose from
        :param end: <int> the maximum number to choose from
        :return <int> A random integer
        """
        return random.randint(beg, end)
  
As shown in this example, in order to be registrable, the function must carry a docstring (a comment immediately after the function definition, delimited by ``"""`` or ``'''``) with the following elements:

* ``:name <package.name_to_register>`` - this is the name the function will take inside the registry and it will typically be different from the local name used when invoking the ``register_fn`` command,
* zero or more ``:param <param_name>: < <type> > [Description]`` providing information about the function's parameters, and
* a single ``:return <type> [Text description]`` providing information about the return type of the function.

Registering the above will result in a function specification and an associated function implementation in the Registry. The specification part will come from the metadata, while the implementation part will contain the code of the file. Having registered it, we can make use of the above function as follows::

    >>> from fns.rand import next_rand
    >>> next_rand(1,100)
    14

Registering PEs
---------------
Similar to functions, PEs can be registered with the ``register_pe`` command::

    >>> r.register_pe('tests.testpe.StringConcatenator')
    Registered PE: https://registry.server/pes/2/

As with functions, the ``register_pe`` command will register a PE specification derived from the class docstring, as well as an associated PE implementation, which will contain the whole file pointed to by the parameter passed.

In this example, the StringConcatenator PE is the following::

	from dispel4py.core import GenericPE
    
	class StringConcatenator(GenericPE):
	    """
	    This PE concatenates the strings it accepts as input.
	    :name pes.str.stringconcatenator
	    :input in: [str] <word> An list of string values to be concatenated
	    :output out: <str> The concatenation of the PE's inputs
	    """
    
	    def __init__(self):
	        GenericPE.__init__(self)
	        self._add_input('in')
	        self._add_output('out')
    
	    def _process(self, inputs):
	        outputs = {}
	        outputs["out"] = ''.join(inputs)
	        return outputs

The PE class docstring must contain the following:

* ``:name <package.name_to_register>`` - this is the name the PE will take inside the registry and it will typically be different from the local name used when invoking the ``register_pe`` command,
* one or more input or output connections in the format ``<:input|output> <name>: < <|[stype>]|> > < <dtype> > <Connection description>``

The ``stype`` of a connection, or its *structural* type should ideally reflect its Python type, while the ``dtype`` or *domain type* could be an arbitrary name, a link to a semantic type specification, etc.

When an ``stype`` if surrounded by ``<>`` it denotes a single value per tuple, while when it is surrounded by ``[]``, it denotes an array of values per tuple.

Having registered the above PE, we can import it as follows::

    >>> from pes.str.stringconcatenator import StringConcatenator

Registering Literals
--------------------
Due to the fact that literals are currently implemented simply as name, value pairs in the Registry, we can register literals straight from the interface without having to first have and document them in separate files::

    r.register_literal(pckg='literals', name='pi',
                       value = '3.1415926535897932384626433832795028841971693',
                       description='The pi constant')

In ``register_literal``, ``pckg.name`` will be the name the literal is registered under. The ``value`` will be the literal value in string format.

The literal above can be imported and used as follows::

    >>> from literals.pi import pi
    >>> print pi
    3.1415926535897932384626433832795028841971693

