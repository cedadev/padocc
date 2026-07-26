
def pytest_collection_modifyitems(items):

    CLASS_ORDER = [
        "TestSetup",
        "TestFHs",
        "TestInit",
        "TestScan",
        "TestCompute",
        "TestZarrCompute",
        "TestZarrValidate",
        "TestValidate",
        "TestGroup",
        # Icechunk last: running a phase persists the project's cloud_format,
        # so these would otherwise leave TestValidate and TestGroup pointed at
        # the icechunk product rather than the one they were written for.
        "TestIcechunkPrefixes",
        "TestIcechunkCompute",
        "TestIcechunkStoreLayout",
        "TestIcechunkValidate",
        #"TestProject"
    ]

    sorted_items = items.copy()
      # read the class names from default items
    class_mapping = {item: item.cls.__name__ for item in items}

    # Iteratively move tests of each class to the end of the test queue
    for class_ in CLASS_ORDER:
        sorted_items = [it for it in sorted_items if class_mapping[it] != class_] + [
            it for it in sorted_items if class_mapping[it] == class_
        ]
        
    items[:] = sorted_items