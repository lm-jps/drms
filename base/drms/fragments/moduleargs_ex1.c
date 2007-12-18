/*
 * The first three all declare an empty arguments list. The next three all
 * declare an arguments list with a single key-value pair. The last declares
 * an arguments list with two key-value pairs.
 */

ModuleArgs_t *module_args;
ModuleArgs_t module_args[] = {};
ModuleArgs_t module_args[] = {
  {ARG_END}
};

ModuleArgs_t module_args[] = {
  {ARG_STRING, "name_0", "default value", "comment"},
  {}
};

ModuleArgs_t module_args[] = {
  {ARG_STRING, "name_0", "default value", "comment"},
  {},
  {ARG_STRING, "name_1", "default value", "comment"}
};

ModuleArgs_t module_args[] = {
  {ARG_STRING, "name_0", "default value", "comment"},
  {ARG_END},
  {ARG_STRING, "name_1", "default value", "comment"}
};

ModuleArgs_t module_args[] = {
  {ARG_STRING, "name_0", "default value 0", "comment"},
  {ARG_STRING, "name_1", "default value 1", "comment"},
  {}
};

ModuleArgs_t module_args[] = {
  {ARG_STRING, "message",    "Hello, world"},
  {ARG_STRING, "file",       "",             "filename (required)"},
  {ARG_INT,    "iterations", "1",            "number of iterations",
      "[0"},
  {ARG_FLOAT,  "scale",      "1.0",          "scaling factor",
      "(0.0"},
  {ARG_TIME,   "time",       "2009.01.01_08:00_UT",
      "image observation time"},
  {ARG_NUME,   "projection", "Mercator",     "mapping projection"
      "orthographic, stereographic, Mercator,Postels,gnomonic"},
  {ARG_FLAG,   "v",          "",             "run in verboses mode"},
  {}
};
