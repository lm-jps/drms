DRMS_KeyMap_t *km = drms_keymap_create();
char buf[DRMS_MAXNAMELEN];

if (km)
{
   if (drms_keymap_parsetable(km, text))
   {
      drms_keyword_getintname_ext("CTYPE", NULL, km, drmskeyname, sizeof(buf));
      /* Do something with drmskeyname*/
   }

   drms_keymap_destroy(&km);
}
