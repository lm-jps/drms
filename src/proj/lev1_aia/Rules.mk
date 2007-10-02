sp 		:= $(sp).x
dirstack_$(sp)	:= $(d)
d		:= $(dir)

# Subdirectories. Directory-specific rules are optional here. The
# order NOT matter.

# Standard things
d		:= $(dirstack_$(sp))
sp		:= $(basename $(sp))
