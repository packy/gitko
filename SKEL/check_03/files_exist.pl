#!/app/clarity/perl/bin/perl
use strict;
use warnings;

# because we want to import an additional subroutine, we
# specify :DEFAULT (to import all the default subroutines)
# and then the name of the sub we're importing
use ComplianceUtils qw( :DEFAULT parameter_to_list );

my @files = parameter_to_list('LIST_OF_EXISTING_FILES');

die "The rule didn't get list of files to check"
	unless scalar @files;

foreach my $file (@files) {
	fail_compliance("File '$file' doesn't exist")
		unless -f $file;
}

pass_compliance("All files from the input list exist");
