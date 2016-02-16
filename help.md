# Coalesce Step - Help

The Coalesce Transformation step selects the first non null value from a group of input fields and passes it down the stream or returns null if all the fields are null.

If all input fields have the same data type then the output will reflect that, otherwise the output will have a more generic String data type.

## The Interface

A description of the options available in this step:

| Option                                 | Definition                                                         |
|----------------------------------------|--------------------------------------------------------------------|
| Step name                              | Name of this step as it appears in the transformation workspace    |
| OutputField                            | The name of the new field in which to store the step result        |
| Field A                                | First input field                                                  |
| Field B                                | Second input field                                                 |
| Field C                                | Third input field                                                  |