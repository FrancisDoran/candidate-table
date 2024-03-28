from socrata.authorization import Authorization
from socrata import Socrata
import os
import sys

auth = Authorization(
  'data.oaklandca.gov',
  os.environ['OAKDATA_KEY'],
  os.environ['OAKDATA_SECRET']
)

socrata = Socrata(auth)
view = socrata.views.lookup('v93t-prrc')

with open('output/output.csv', 'rb') as my_file:
  (revision, job) = socrata.using_config('output_03-28-2024_01d1', view).csv(my_file)
  # These next 2 lines are optional - once the job is started from the previous line, the
  # script can exit; these next lines just block until the job completes
  job = job.wait_for_finish(progress = lambda job: print('Job progress:', job.attributes['status']))
  sys.exit(0 if job.attributes['status'] == 'successful' else 1)
