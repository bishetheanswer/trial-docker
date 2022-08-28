#!/bin/bash
prompt="Select a flow you want to register:"
options=( $(find flows/*.py  -maxdepth 1 -print0 | xargs -0) )

PS3="$prompt "
select opt in "${options[@]}" "Quit" ; do 
    if (( REPLY == 1 + ${#options[@]} )) ; then
        exit

    elif (( REPLY > 0 && REPLY <= ${#options[@]} )) ; then
        echo -e  "Registering\033[1;92m $opt\033[0m in\033[1;92m authors-homogenization\033[0m project"
		break

    else
        echo "Invalid option. Try another one."
    fi
done

prefect register -p $opt --project authors-homogenization