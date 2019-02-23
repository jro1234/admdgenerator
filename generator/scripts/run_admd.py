#!/usr/bin/env/python

'''
    Usage:
           $ python run_admd.py [name] [additional options]

    Timestamp format:
       >>>  TIMER <object> <label> <time>

     - object: single word, what we are monitoring
     - label: a few words, description of timestamp
     - timestamp: float, the time

      $ if 'TIMER' in timestampline:
      $     timestamp = timestampline.split('TIMER')[-1].strip().split()
      $     obj = str(timestamp[0])
      $     label = [str(l) for l in timestamp[1:-1]]
      $     t = float(timestamp[-1])

'''

# Import custom adaptivemd init & strategy functions
import traceback
import sys
import time

import uuid

from _argparser import argparser
from __run_admd import init_project, strategy_function, get_logger, formatline, pythontask_callback

#dump_finalize_timestamps = False
dump_finalize_timestamps = True
if dump_finalize_timestamps:
    from __run_tools import pull_final_timestamps
    from pprint import pformat

from adaptivemd import Task, TrajectoryGenerationTask, TrajectoryExtensionTask, PythonTask, Scheduler

logger = get_logger(logname=__name__)#, logfile='run_admdrp.'+__name__+'.log')
exitval = -1

uuid_from_long = lambda x: uuid.UUID(int=x.__uuid__)


# FIXME tracking state groups here needs improvement
# success, cancelled, fail, halted
# -  no need for dummy since it will always
#    be an existing task, and not checked
final_states = Task.FINAL_STATES + Task.RESTARTABLE_STATES
task_done = lambda ta: ta.state in final_states

created_state = 'created'

fix_states = {'queued','running','fail','failed','cancelled','pending'}
runnable_states = set(fix_states)
runnable_states.add(created_state)

created_state_update = {"state":created_state, "__dict__.state":created_state}

is_incomplete = lambda ta: ta.state in runnable_states

def on_model_fail(project, failed_model_task, incr=1):
    new_stride = failed_model_task.__dict__['_python_kwargs']['tica_stride'] + incr
    logger.info("Increasing model strides to hasten TICA/clustering calculations")
    logger.info("Old stride: {0}, New strides: {1}".format(new_stride-incr, new_stride))
    if is_incomplete(failed_model_task):
        project.tasks._set._document.find_one_and_update(
            {"_id" : uuid_from_long(failed_model_task)},
            {"$set": {"_dict._python_kwargs.tica_stride" : new_stride,
                      "_dict._python_kwargs.clust_stride": new_stride}},
        )


#class MDWatcher(object):
#    def __init__(self, project, tasks=list()):
#        super(MDWatcher, self).__init__()
#
#        self._project  = project
#        self.tasks     = tasks
#        self._task_ids = list()
#        self._dirs     = list()
#
#    @property
#    def tasks(self):
#        return self._tasks
#
#    @tasks.setter
#    def tasks(self, tasks=list():
#        assert isinstance(tasks, (list,set,tuple))
#        self._tasks = tasks
#
#    def find_dirs(self):
#        if not self._task_ids:
#            self.
#        while not self._dirs:
#            
#
#    def start_watching(self):
        


def calculate_request(size_workload, n_workloads, n_steps, steprate=1000):
    '''
    Calculate the parameters for resource request done by RP.
    The workload to execute will be assessed to estimate these
    required parameters.
    -- Function in progress, also requires a minimum time
       and cpus per node. Need to decide how these will be
       calculated and managed in general.

    Parameters
    ----------
    size_workload : <int>
    For now, this is like the number of nodes that will be used.
    With OpenMM Simulations and the 1-node PyEMMA tasks, this is
    also the number of tasks per workload. Clearly a name conflict...

    n_workloads : <int>
    Number of workload iterations or rounds. The model here
    is that the workloads are approximately identical, maybe
    some will not include new modeller tasks but otherwise
    should be the same.

    n_steps : <int>
    Number of simulation steps in each task.

    steprate : <int>
    Number of simulation steps per minute. Use a low-side
    estimate to ensure jobs don't timeout before the tasks
    are completed.
    '''
    # TODO get this from the configuration and send to function
    rp_cpu_overhead = 16
    cpu_per_node = 16
    cpus = size_workload * cpu_per_node + rp_cpu_overhead
    # nodes is not used
    nodes = size_workload 
    gpus = size_workload
    logger.info(formatline(
        "\nn_steps: {0}\nn_workloads: {1}\nsteprate: {2}".format(
                         n_steps, n_workloads, steprate)))

    # 15 minutes padding for initialization & such
    # as the minimum walltime (should take ~3)
    wallminutes = 15 + int(float(n_steps) * n_workloads / steprate)
    #wallminutes = 15 + int(float(n_steps) * n_workloads / steprate)

    return cpus, nodes, wallminutes, gpus 


if __name__ == '__main__':

    project = None
    sleeptime = 1

    try:

        parser   = argparser()
        args     = parser.parse_args()

        using_rp = args.rp
        submit_only = args.submit_only

        if using_rp:
            from adaptivemd.rp.client import Client

        logger.info("Initializing Project named: " + args.project_name)
        # send selections and frequencies as kwargs
        #fix1#project = init_project(p_name, del_existing, **freq)
        logger.info(formatline("\n{}".format(args)))
        logger.info(formatline("TIMER Project opening {0:.5f}".format(time.time())))
        project = init_project(args.project_name,
                               args.system_name,
                               args.all,
                               args.prot,
                               args.platform,
                               reinitialize=args.reinitialize,
                               #args.dblocation
                              )

        logger.info(formatline("TIMER Project opened {0:.5f}".format(time.time())))
        logger.info("AdaptiveMD dburl: {}".format(project.storage._db_url))
        logger.info("project.trajectories: {}".format(len(project.trajectories)))
        logger.info("project.models: {}".format(len(project.models)))

        rescue_tasks = filter(is_incomplete, project.tasks)
        rescue_uuids = list(map(lambda ta: ta.__uuid__, rescue_tasks))
        n_incomplete_tasks = len(rescue_tasks)
        if args.rescue_tasks:
            if n_incomplete_tasks:
                exitval = n_incomplete_tasks
                logger.info(
                  "Exiting to rescue {} incomplete tasks".format(exitval))
                sys.exit(exitval)

            elif args.rescue_only:
                exitval = 0
                logger.info("All tasks rescued, exiting with no action")
                sys.exit(exitval)

            else:
                logger.info("Proceeding with given workload after rescue check, found {} incomplete tasks".format(n_incomplete_tasks))
        else:
            logger.info("No rescue check performed, found {} incomplete tasks".format(n_incomplete_tasks))

        steprate = 1000
        #if args.system_name.startswith('chignolin'):
        #    steprate = 13500
        #elif args.system_name.startswith('ntl9'):
        #    steprate = 7000
        #elif args.system_name.startswith('villin'):
        #    steprate = 9000
        #elif args.system_name.startswith('bba'):
        #    steprate = 12000
        if  args.longts: # TODO TESTME this logic seems reversed
            steprate *= 2

        if args.init_only:
            logger.info("Leaving project '{}' initialized without tasks".format(project.name))

        else:
            logger.info("Adding event to project: {0}".format(project.name))

            # FIXME TODO engine selection by name
            if  args.longts:
                ext = '-5'
            else:
                ext = '-2'

            nm_engine = 'openmm' + ext
            engine = project.generators[nm_engine]

            # implement `get` method on `Bundle` and return None if no match
            n_traj   = args.n_traj
            n_rounds = args.n_rounds # for multiround runtime
            round_n  = args.round_n  # to externally track
            length   = args.length
            modeller = None
            logger.info("Configuring workload")
            model_tasks = list(project.tasks.c(PythonTask).m('state','created'))
            if args.modeller:
                nm_modeller = args.modeller + ext
                modeller = project.generators[nm_modeller]

            if not n_traj:
                logger.info("No trajectories given to run")
                if not modeller:
                    logger.info("No modeller either, looking to do a cleanup")
                    # TODO FIXME this is not acceptable for general use...
                    # TODO FIXME use rescue_uuids, then these can be made
                    #            in different ways in the future
                    logger.info("before fixes: observed task states: {}".format(project.task_states))
                    for fix_state in fix_states:
                        project.tasks._set._document.update_many(
                          {"state": fix_state},
                          {"$set" : created_state_update}
                        )
                        #project.tasks._set._document.update_many(
                        #  {"state": fix_state, "_cls":"PythonTask"},
                        #  {"$set" : created_state_update}
                        #)
                        #project.tasks._set._document.update_many(
                        #  {"state": fix_state, "_cls":"TrajectoryGenerationTask"},
                        #  {"$set" : created_state_update}
                        #)
                        #project.tasks._set._document.update_many(
                        #  {"state": fix_state,"_cls":"TrajectoryExtensionTask"},
                        #  {"$set": created_state_update}
                        #)

                    logger.info("after fixes: observed task states: {}".format(project.task_states))
                    # TODO use sets, order should not come up
                    #project.tasks._set.clear_cache()
                    #project.tasks._set.load_indices()
                    logger.info("after reload: observed task states: {}".format(project.task_states))

                    # FIXME right now, identical to rescue_tasks with no reloading either
                    new_tasks    = project.tasks.v(lambda ta: ta.__uuid__ in rescue_uuids)
                    model_tasks += list(new_tasks.c(PythonTask))
                    #new_tasks   = list(project.tasks.m('state', created_state))
                    #model_tasks = list(project.tasks.c(PythonTask).m('state', created_state))
                    #new_tasks   = list(project.tasks.c(TrajectoryExtensionTask).m('state',created_state))\
                    #            + list(project.tasks.c(TrajectoryGenerationTask).m('state',created_state))\
                    #            + list(project.tasks.c(PythonTask).m('state',created_state))

                    # TODO see if this still happens
                    # list elements getting duplicated somehow
                    new_tasks = list(set(new_tasks))
                    model_tasks = list(set(model_tasks))
                    logger.info("Found {} tasks to execute in cleanup".format(len(new_tasks)))
                  #  if len(model_tasks) == 1:
                  #      logger.info("Performing on-fail operation to the model task")
                  #      logger.info("Incrementing stride arguments to tica and clustering")
                  #      on_model_fail(project, model_tasks[0], incr=1) #Increments the stride by incr

                    if not new_tasks:
                        print 0
                        sys.exit(0)

                    n_rounds  = 1
                    n_traj    = len(new_tasks)
                    for ta in new_tasks:
                        logger.info("{0}  {1}".format(ta.state, getattr(ta, 'trajectory', None)))
                    if any([hasattr(ta, 'trajectory') for ta in new_tasks]):
                        # doesn't handle TrajectoryExtensionTask correctly
                        #length    = max(map(lambda ta: ta.trajectory.length, filter(lambda ta: hasattr(ta,'trajectory'), new_tasks)))
                        length = args.length
                else:
                    # FIXME TODO need a 'held' state for tasks so holdovers in 'created'
                    #            state don't start when we're looking to do a model task
                    pass


            # TODO add rp definition
            logger.info("CALCULATING REQUEST")
            #print args.n_traj, args.modeller, int(bool(args.modeller))
            cpus, nodes, walltime, gpus = calculate_request(
                 n_traj + int(bool(modeller)),
                 n_rounds,
                 length,
                 steprate,
            )

            if args.minutes:
                walltime = args.minutes

            sfkwargs = dict()
            sfkwargs['num_macrostates'] = 25

            logger.info(formatline(
              "\nResource request arguments: \ncpus: {0}\nwalltime: {1}\ngpus: {2}".format(
              cpus, walltime, gpus)))
            logger.info("n_rounds: {}".format(args.n_rounds))

            project.request_resource(cpus, walltime, gpus, 'current')

            client = None
            if using_rp:
                logger.info("starting RP client for workflow execution")
                client = Client(project.storage._db_url, project.name)
                # NOTE!! SENDING CLIENT TO STRATEGY FUNCTION TO START
                #client.start()

            if args.n_traj or args.modeller:
                # Tasks not in this list will be checked for
                # a final status before stopping RP Client
                existing_tasks = [ta.__uuid__ for ta in project.tasks]

                logger.info("Project event adding from {}".format(strategy_function))
                logger.info(formatline("TIMER Project event adding {0:.5f}".format(time.time())))

                project.add_event(strategy_function(
                    project, engine, n_traj,
                    args.n_ext, length, round_n,
                    modeller=modeller,
                    fixedlength=True,#args.fixedlength,
                    minlength=args.minlength,
                    n_rounds=n_rounds,
                    environment=args.environment,
                    activate_prefix=args.activate_prefix,
                    virtualenv=args.virtualenv,
                    longest=args.all,
                    cpu_threads=args.threads,
                    sampling_method=args.sampling_method,
                    startontraj=args.after_n_trajs,
                    min_model_trajlength=args.min_model_trajlength,
                    batchsleep=args.batchsleep,
                    batchsize=args.batchsize,
                    batchwait=args.batchwait,
                    progression=args.progression,
                    sfkwargs=sfkwargs,
                    rp_client=client,
                    ))

                new_tasks = filter(lambda ta: ta.__uuid__ not in existing_tasks, project.tasks)

            else:
                logger.info("Project event adding from incomplete tasks\n{}".format(new_tasks))
                sleeptime = 20
                # TODO check on below... expect is_done to take a while
                #      on (all) non-failing tasks, should it be yeild
                #      instead of return?
                # FIXME this event finishes immediately for some reason...
                #         - event usage description needed
               # def cleanup_event(tasks):
               #     logger.info([ta.state for ta in tasks])
               #     return  all([ta.is_done() for ta in tasks])
               # project.add_event(lambda: cleanup_event)
                if using_rp:
                    client.start()

                logger.info("These are the tasks being cleaned up:")
                logger.info(pformat(new_tasks))
                project.add_event(all([ta.is_done() for ta in new_tasks]))

            logger.info(formatline("TIMER Project event added {0:.5f}".format(time.time())))

            if submit_only:
                exitval = 0
                print exitval
                sys.exit(exitval)

            logger.info("TRIGGERING PROJECT")
            #watcher = launch_watcher(project, new_tasks)
            project.wait_until(project.events_done)
            logger.info(formatline("TIMER Project event done {0:.5f}".format(time.time())))
            done = False
            while not done:
                logger.info("Waiting for final state assignments to new states")
                time.sleep(sleeptime)
                if all([task_done(ta) for ta in new_tasks]):
                    done = True
                    logger.info("All new tasks finalized")
                    logger.info(formatline("TIMER Project tasks checked {0:.5f}".format(time.time())))

            if using_rp:
                client.stop()

            else:
                logger.info("TRYING TO SHUTDOWN WORKERS")
                logger.info(project.workers.all)
                project.workers.all.execute('shutdown')
                #[setattr(w,'state','down') for w in project.workers]
                logger.info(project.workers.all)

            if model_tasks:
                logger.info("Synchronizing any new model data now")
                logger.info("Number of models: {}".format(len(project.models)))
                scd = Scheduler(project.resources.one)
                scd.enter(project)
                for ta in model_tasks:
                    logger.info("Task output stored?: {}".format(ta.output_stored))
                    if ta.state == 'success':
                        logger.info("Synching data from this task: {}".format(ta))
                        logger.info("pythontask_callback(ta, scd, '__main__')")
                        pythontask_callback(ta, scd, '__main__')
                    else:
                        logger.info("This analysis cleanup didn't complete... {}".format(ta))

                    try:
                        logger.info("Model Task stdout: ")
                        logger.info(pformat(ta.stdout))
                        logger.info("Model Task stderr: ")
                        logger.info(pformat(ta.stderr))
                    except ValueError:
                        # FIXME why do they not sync after task sometimes?
                        logger.info("Cannot find model task output logs")

                logger.info("Number of models now: {}".format(len(project.models)))

            logger.info("Something about new tasks should pop up")
            logger.info(formatline("{}".format([nta.state for nta in new_tasks])))

            project.resources.consume_one()
            if all(map(lambda nta: nta.state == 'success', new_tasks)):
                logger.info('exiting with success')
                exitval = 0

    except KeyboardInterrupt:
        logger.info("KEYBOARD INTERRUPT- Quitting Workflow Execution")

    except Exception as e:
        logger.error("Error during workflow: {}".format(e))
        logger.error(traceback.print_exc())

    finally:

        if project:
            project.resources.consume_one()
            project.close()
         #   if not args.init_only and dump_finalize_timestamps:
         #       final_timestamps = pull_final_timestamps(project)
         #       logger.info(pformat(final_timestamps))

        logger.info("Exiting Event Script")
        logger.info(formatline("TIMER Project closed {0:.5f}".format(time.time())))

        if len(project.workers) > 0:
            logger.info("TRYING TO SHUTDOWN WORKERS")
            logger.info(project.workers.all)
            project.workers.all.execute('shutdown')
            #[setattr(w,'state','down') for w in project.workers]
            logger.info(project.workers.all)

        print exitval
        sys.exit(exitval)


