echo "# this file is located in 'src/root_command.sh'"
echo "# you can edit it freely and regenerate (it will not be overwritten)"
inspect_args

# TODO: parse args and make.
echo "artifact: graphscope_flex_${args[--app]}_"+${$comps// /_}+".deb is built."  
