#include "jsmn.h"
#include "processing.h"

HContainer_t *g_processing_steps = NULL;
HContainer_t *g_processing_step_public_names = NULL;

/* processing argument handler-function type */
typedef void *(*p_fn_size_ratio_handler)(void *data);

static void free_step_node(const void *data)
{
    struct _processing_step_node_data_ *node_data = (struct _processing_step_node_data_ *)data;
    hcon_free(&node_data->arguments);
}

/* define supported processing steps */
#define PROCESSING_STEP_DATA(X, Y) #X,
char *PROCESSING_STEPS[] =
{
   #include "processing.h"
   "end"
};
#undef PROCESSING_STEP_DATA

/* define processing step public names */
#define PROCESSING_STEP_DATA(X, Y) #Y,
char *PROCESSING_STEP_PUBLIC_NAMES[] =
{
   #include "processing.h"
   "end"
};
#undef PROCESSING_STEP_DATA


/* define processing-step handler functions */
#define PROCESSING_STEP_DATA(X, Y) static void *X ## _handler(void *data);
  #include "processing.h"
#undef PROCESSING_STEP_DATA

/* define array of pointers to the processing-step handler functions */
#define PROCESSING_STEP_DATA(X, Y) X ## _handler,
p_fn_size_ratio_handler PROCESSING_HANDLERS[] =
{
   #include "processing.h"
   NULL
};
#undef PROCESSING_STEP_DATA

/* PROCESSING-STEP HANDLER FUNCTIONS
 */

static void *aia_scale_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    /* data passed in */
    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;

    operation = (char *)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
       if (strcasecmp(operation, "special_args") == 0)
       {
           /* return the special args for this step (there are no special args for this step) */
           return special_args;
       }
    }

    ratio = 1.0;

    return &ratio;
}

static void *aia_scale_orig_handler(void *data)
{
    return aia_scale_handler(data);
}

static void *aia_scale_aialev1_handler(void *data)
{
    return aia_scale_handler(data);
}

static void *aia_scale_other_handler(void *data)
{
    return aia_scale_handler(data);
}

static void *to_ptr_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    /* data passed in */
    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;

    operation = *(char **)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
        if (strcasecmp(operation, "special_args") == 0)
        {
            /* return the special args for this step (there are no special args for this step) */
            return special_args;
        }
    }

    ratio = 1.0;

    return &ratio;
}

static void *resize_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    /* data passed in */
    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;
    char *target_scale_str = NULL;
    float target_scale = -1;
    char *template_rec_str = NULL;
    DRMS_Record_t *template_rec = NULL;
    int drms_status = DRMS_SUCCESS;
    DRMS_RecordSet_t *open_records = NULL;
    float cdelt1 = -1;

    operation = (char *)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
        if (strcasecmp(operation, "special_args") == 0)
        {
            /* return the special args for this step */
            if (!special_args)
            {
                special_args = calloc(1, sizeof(Hash_Table_t));
                hash_init(special_args, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                hash_insert(special_args, "scale_to", "T");
            }

            return special_args;
        }
    }

    target_scale_str = (char *)hcon_lookup_lower(arguments, "scale_to");
    sscanf(target_scale_str, "%f", &target_scale);
    template_rec_str = (char *)hcon_lookup_lower(arguments, "template_rec");
    sscanf(template_rec_str, "%p", &template_rec);

    /* get newest record from series */
    /* ugh - cannot specify both a key list and n=XX, so we have to fetch all keys */
    open_records = drms_open_records2(template_rec->env, template_rec->seriesinfo->seriesname, NULL, 0, -1, 0, &drms_status);

    if (open_records && drms_status == DRMS_SUCCESS)
    {
        cdelt1 = drms_getkey_float(open_records->records[0], "cdelt1", &drms_status);
        drms_close_records(open_records, DRMS_FREE_RECORD);
    }
    else
    {
        fprintf(stderr, "[ resize_handler ] unable to open most recent record in series `%s`\n", template_rec->seriesinfo->seriesname);
        ratio = -1;
    }

    if (target_scale == 0 || !isfinite(cdelt1))
    {
        ratio = -1;
    }
    else
    {
        ratio = cdelt1 / target_scale;
    }

    return &ratio;
}

static void *im_patch_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    /* data passed in */
    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;
    char *new_dim_x_str = NULL;
    int new_dim_x = -1;
    char *new_dim_y_str = NULL;
    int new_dim_y = -1;
    char *segment_str = NULL;
    DRMS_Segment_t *segment = NULL;
    int orig_dim_x = -1;
    int orig_dim_y = -1;

    operation = (char *)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
        if (strcasecmp(operation, "special_args") == 0)
        {
            /* return the special args for this step */
            if (!special_args)
            {
                special_args = calloc(1, sizeof(Hash_Table_t));
                hash_init(special_args, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                hash_insert(special_args, "width", "T");
                hash_insert(special_args, "height", "T");
            }

            return special_args;
        }
    }

    new_dim_x_str = (char *)hcon_lookup_lower(arguments, "width");
    sscanf(new_dim_x_str, "%d", &new_dim_x);
    new_dim_y_str = (char *)hcon_lookup_lower(arguments, "height");
    sscanf(new_dim_y_str, "%d", &new_dim_y);
    segment_str = (char *)hcon_lookup_lower(arguments, "segment");
    sscanf(segment_str, "%p", &segment);

    if (segment)
    {
        orig_dim_x = segment->axis[0]; /* from first record's segment dims */
        orig_dim_y = segment->axis[1]; /* from first record's segment dims */

        if (orig_dim_x == 0 || orig_dim_y == 0)
        {
            ratio = -1;
        }
        else
        {
            ratio = (float)(new_dim_x * new_dim_y) / (float)(orig_dim_x * orig_dim_y);
        }
    }
    else
    {
        fprintf(stderr, "[ im_patch_handler ] unable to locate first segment\n");
        ratio = -1;
    }

    return &ratio;
}

static void *map_proj_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    /* data passed in */
    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;
    char *new_dim_x_str = NULL;
    int new_dim_x = -1;
    char *new_dim_y_str = NULL;
    int new_dim_y = -1;
    char *segment_str = NULL;
    DRMS_Segment_t *segment = NULL;
    int orig_dim_x = -1;
    int orig_dim_y = -1;

    operation = (char *)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
        if (strcasecmp(operation, "special_args") == 0)
        {
            /* return the special args for this step */
            if (!special_args)
            {
                special_args = calloc(1, sizeof(Hash_Table_t));
                hash_init(special_args, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                hash_insert(special_args, "cols", "T");
                hash_insert(special_args, "rows", "T");
            }

            return special_args;
        }
    }

    new_dim_x_str = (char *)hcon_lookup_lower(arguments, "cols");
    sscanf(new_dim_x_str, "%d", &new_dim_x);
    new_dim_y_str = (char *)hcon_lookup_lower(arguments, "rows");
    sscanf(new_dim_y_str, "%d", &new_dim_y);
    segment_str = (char *)hcon_lookup_lower(arguments, "segment");
    sscanf(segment_str, "%p", &segment);

    if (segment)
    {
        orig_dim_x = segment->axis[0]; /* from first record's segment dims */
        orig_dim_y = segment->axis[1]; /* from first record's segment dims */

        if (orig_dim_x == 0 || orig_dim_y == 0)
        {
            ratio = -1;
        }
        else
        {
            ratio = (float)(new_dim_x * new_dim_y) / (float)(orig_dim_x * orig_dim_y);
        }
    }
    else
    {
        fprintf(stderr, "[ map_proj_handler ] unable to locate first segment\n");
        ratio = -1;
    }

    return &ratio;
}

static void *rebin_handler(void *data)
{
    static Hash_Table_t *special_args = NULL; /* just the names of the special arguments, not the values */
    static float ratio = -1;

    HContainer_t *arguments = (HContainer_t *)data;
    char *operation = NULL;
    char *scaling_factor_str = NULL;
    float scaling_factor = -1;

    operation = (char *)hcon_lookup_lower(arguments, "operation");

    if (operation)
    {
        if (strcasecmp(operation, "special_args") == 0)
        {
            /* return the special args for this step */
            if (!special_args)
            {
                special_args = calloc(1, sizeof(Hash_Table_t));
                hash_init(special_args, 89, 0, (int (*)(const void *, const void *))strcmp, hash_universal_hash);
                hash_insert(special_args, "scale", "T");
            }

            return special_args;
        }
    }

    scaling_factor_str = (char *)hcon_lookup_lower(arguments, "scale");
    sscanf(scaling_factor_str, "%f", &scaling_factor);

    ratio = scaling_factor * scaling_factor;

    return &ratio;
}

/* END PROCESSING-STEP HANDLER FUNCTIONS
 */

/* in the global `g_processing_steps` associative array, store the index of each processing step listed in PROCESSING_STEPS;
 * returns the index for processing-step `step` if `step` is a valid processing step; returns -1 otherwise
 */
int get_processing_step_index(const char *step)
{
    int step_index = -1;
    void *found = NULL;
    int err = 0;

    if (!g_processing_steps)
    {
        /* create - key --> name, val --> index into PROCESSING_STEPS, PROCESSING_HANLDERS, PROCESSING_ARGUMENTS */
        g_processing_steps = hcon_create(sizeof(int), PROCESSING_STEP_NAME_LEN, NULL, NULL, NULL, NULL, 0);

        if (g_processing_steps)
        {
            /* populate */
            step_index = 0;
            while (strcmp(PROCESSING_STEPS[step_index], "end") != 0)
            {
                hcon_insert_lower(g_processing_steps, PROCESSING_STEPS[step_index], &step_index);
                step_index++;
            }
        }
        else
        {
            err = 1;
        }

        if (!err)
        {
            if (g_processing_step_public_names)
            {
                /* populate */
                step_index = 0;
                while (strcmp(PROCESSING_STEP_PUBLIC_NAMES[step_index], "end") != 0)
                {
                    hcon_insert_lower(g_processing_step_public_names, PROCESSING_STEPS[step_index], &step_index);
                    step_index++;
                }
            }
            else
            {
                err = 1;
            }
        }
    }

    /* search by offical name */
    if (g_processing_steps)
    {
        if ((found = hcon_lookup_lower(g_processing_steps, step)) != NULL)
        {
            step_index = *(int *)found;
        }
    }

    if (!found && g_processing_step_public_names)
    {
        /* search by public name */
        if ((found = hcon_lookup_lower(g_processing_steps, step)) != NULL)
        {
            step_index = *(int *)found;
        }
    }

    return step_index;
}

/* calls the processing-step handler for processing step `step` with the `operation` argument of `special_args` to
 * obtain the Hash_Table_t of processing-step program special arguments (the program arguments needed for a size-
 * ratio calculation); returns the hash table if it is located, or NULL otherwise */
static Hash_Table_t *get_special_args(const char *step)
{
    int step_index = -1;
    p_fn_size_ratio_handler handler = NULL;
    HContainer_t arguments;
    char value[PROCESSING_HANDLER_ARGUMENT_VALUE_LEN] = {0};
    Hash_Table_t *special_args = NULL;

    /* get index */
    step_index = get_processing_step_index(step);

    if (step_index >= 0)
    {
        /* get handler */
        handler = PROCESSING_HANDLERS[step_index];

        /* add `operation` and `special_args` */
        hcon_init(&arguments, PROCESSING_HANDLER_ARGUMENT_VALUE_LEN, PROCESSING_HANDLER_ARGUMENT_NAME_LEN, NULL, NULL);
        snprintf(value, sizeof(value), "special_args");
        hcon_insert_lower(&arguments, "operation", value);

        /* get and return set of special args */
        special_args = (Hash_Table_t *)handler((void *)&arguments);
        hcon_free(&arguments);
        return special_args;
    }
    else
    {
        fprintf(stderr, "[ get_special_args ] unable to get special_args for processing step %s\n", step);
        return NULL;
    }

    return NULL;
}

/* parses the JSON `processing` program argument; for each step in this argument, extracts the special arguments
 * (the program arguments needed for a size-ratio calculation) and returns, in a list, an associative array of
 * these arguments' key-value pairs; each node in the returned list contains the associate array for a single
 * processing step
 */
int get_processing_list(const char *processing_json, LinkedList_t **processing_list)
{
    /* validate and insert processing-step info from processing argument */
    int err = 0;
    LinkedList_t *list = NULL;
    jsmn_parser parser;
    jsmntok_t *tokens = NULL;
    size_t sz_jstokens = 1024;
    jsmnerr_t parse_result = 0;
    int token_index = -1;
    int num_steps = -1;
    char step_name[PROCESSING_STEP_NAME_LEN] = {0};
    struct _processing_step_node_data_ node = {0}; /* stays empty - a template of sorts */
    ListNode_t *list_node = NULL;
    struct _processing_step_node_data_ *node_data = NULL; /* data inside a list node */
    int property_obj_token_index = -1;
    jsmntok_t *property_obj_token = NULL;
    char property_name[PROCESSING_STEP_PROPERTY_NAME_LEN];
    char property_value[PROCESSING_STEP_PROPERTY_VALUE_LEN];

    /* processing is a json object that represents a set of processing steps; each step is represented by a an object that
     * contains the keys/values:
     *   {
     *      "resize" :
     *      {
     *          "regrid" : true,
     *          "do_stretchmarks" : false,
     *          "center_to" : false,
     *          "rescale" : true,
     *          "scale_to" : 2
     *      },
     *      "map_proj" :
     *      {
     *          "map" : "carree",
     *          "clon" : 350.553131,
     *          "clat" : 69,
     *          "scale" : 0.0301,
     *          "cols" : 22,
     *          "rows" : 44
     *      }
     *   }
     */

    tokens = calloc(1, sizeof(jsmntok_t) * sz_jstokens);

    if (tokens)
    {
        jsmn_init(&parser);
        parse_result = jsmn_parse(&parser, processing_json, tokens, sz_jstokens);

        if (parse_result == JSMN_ERROR_NOMEM || parse_result == JSMN_ERROR_INVAL || parse_result == JSMN_ERROR_PART)
        {
           /* did not allocate enough tokens to hold parsing results; */
            fprintf(stderr, "[ get_processing_list ] exceeded maximum number of json tokens in processing json\n");
            err = 1;
        }
        else if (parse_result != JSMN_SUCCESS)
        {
            fprintf(stderr, "[ get_processing_list ] invalid json in processing json\n");
            err = 1;
        }
        else
        {
            token_index = 0;

            if (tokens[token_index].type != JSMN_OBJECT)
            {
                /* no root object */
                fprintf(stderr, "[ get_processing_list ] processing json must contain json with a root object\n");
                err = 1;
            }
            else
            {
                /* loop over root objects (one for each processing step); I'm going to guess that
                 * res is the total number of tokens returned */

                /* first step name */
                token_index++;

                /* size() of the root obj is 2x the actual number of processing steps because each step has a name token and
                 * an object token */
                for (num_steps = 0; num_steps * 2 < tokens[0].size; token_index++, num_steps++)
                {
                    snprintf(step_name, sizeof(step_name), "%.*s", tokens[token_index].end - tokens[token_index].start, processing_json + tokens[token_index].start);
                    /* token.end is the index of the char after the last char in the token */

                    /* validate step; creates the mapping from step name to index into PROCESSING_STEPS, PROCESSING_HANDLERS, and PROCESSING_ARGUMENTS */
                    if (get_processing_step_index(step_name) == -1)
                    {
                        fprintf(stderr, "[ get_processing_list ] invalid processing step `%s`\n", step_name);
                        err = 1;
                        break;
                    }
                    else
                    {
                        if (!list)
                        {
                            list = list_llcreate(sizeof(struct _processing_step_node_data_), (ListFreeFn_t)free_step_node);

                            if (!list)
                            {
                                fprintf(stderr, "[ get_processing_list ] out of memory creating list\n");
                                err = 1;
                                break;
                            }
                        }

                        /* insert empty list node */
                        list_node = list_llinserttail(list, &node);

                        /* initialize node for this step */
                        node_data = (struct _processing_step_node_data_ *)list_node->data;
                        snprintf(node_data->step, sizeof(node_data->step), "%s", step_name);
                        hcon_init(&node_data->arguments, PROCESSING_STEP_PROPERTY_VALUE_LEN, PROCESSING_STEP_PROPERTY_NAME_LEN, NULL, NULL);
                    }

                    /* step property object */
                    token_index++;
                    property_obj_token = &tokens[token_index];
                    property_obj_token_index = token_index;

                    if (property_obj_token->type != JSMN_OBJECT)
                    {
                        fprintf(stderr, "[ get_processing_list ] each processing step must have an object of processing-program key-value properties; no such object for `%s`\n", step_name);
                        err = 1;
                        break;
                    }

                    /* examine properties */
                    if (property_obj_token->size > 0)
                    {
                        /* name of first property */
                        token_index++;

                        for (; token_index < property_obj_token_index + property_obj_token->size; token_index++)
                        {
                            snprintf(property_name, sizeof(property_name), "%.*s", tokens[token_index].end - tokens[token_index].start, processing_json + tokens[token_index].start);

                            /* property value */
                            token_index++;

                            /* check for 'true', 'false', 'null' primitives; map 'true' -> "1", 'false' -> "0", 'null' -> "0" for
                             * all processing programs */
                            if (strncmp("true", processing_json + tokens[token_index].start, tokens[token_index].end - tokens[token_index].start) == 0)
                            {
                                snprintf(property_value, sizeof(property_value), "1");
                            }
                            else if (strncmp("false", processing_json + tokens[token_index].start, tokens[token_index].end - tokens[token_index].start) == 0)
                            {
                                snprintf(property_value, sizeof(property_value), "0");
                            }
                            else if (strncmp("null", processing_json + tokens[token_index].start, tokens[token_index].end - tokens[token_index].start) == 0)
                            {
                                snprintf(property_value, sizeof(property_value), "0");
                            }
                            else
                            {
                                snprintf(property_value, sizeof(property_value), "%.*s", tokens[token_index].end - tokens[token_index].start, processing_json + tokens[token_index].start);
                            }

                            hcon_insert_lower(&node_data->arguments, property_name, property_value);
                        }

                        /* we are now pointing at either the name of the next step, or one past last token; the token pointer
                         * will be advanced by 1 when we go to the next iteration of the outer loop, so subtract 1 now */
                        token_index--;
                    }
                }
            }
        }

        free(tokens);
    }

    if (!err)
    {
        *processing_list = list;
    }

    return err;
}

/* for a single processing step, returns the ratio of post-processing image size to pre-processing size;
 * returns -1.0 upon error (which includes non-finite computation values, division by zero, etc.); the
 * node data contain the processing-program arguments needed to calculate the ratio - these data
 * are passed to the processing-step handler; the template record is also passed to the handler, even
 * though the specific handler may not need it */
float get_processing_size_ratio(struct _processing_step_node_data_ *node_data, DRMS_Segment_t *segment)
{
    /* get index */
    int index = -1;
    float ratio = -1;
    p_fn_size_ratio_handler handler = NULL;
    char segment_str[32];

    index = *(int *)hcon_lookup_lower(g_processing_steps, node_data->step);

    if (index >= 0)
    {
        /* get handler */
        handler = PROCESSING_HANDLERS[index];

        /* add template rec to args */
        snprintf(segment_str, sizeof(segment_str), "%p", segment);
        hcon_insert_lower(&node_data->arguments, "segment", segment_str);

        ratio = *(float *)handler((void *)&node_data->arguments);

        /* -1 means a ratio cannot be calculated */
        return ratio;
    }
    else
    {
        fprintf(stderr, "[ get_size_ratio ] unable to get size ratio for processing step %s\n", node_data->step);
        return -1.0;
    }
}
