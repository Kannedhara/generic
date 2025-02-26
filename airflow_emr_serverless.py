######## Class with EMR OUT deferrable ########

class EMR_WITH_OUT_DEFERRABLE:

    def __init__(self, application_id, application_name=None, multiple_runs=False, create_app=False, delete_app=False,
                  create_studio=False, studio_name=None):
        if create_app:
            self.application_id = '0000'
        else:
            self.application_id = application_id
        self.multiple_runs = multiple_runs
        self.application_name = application_name
        self.create_app = create_app
        self.delete_app = delete_app
        self.create_studio = create_studio
        self.studio_name = studio_name

    def run(
            self,
            job_name: str = None,
            job_arguments=None,
            task_id=None
    ):
        """
        :param job_name: Name of the job to be displayed in EMR
        :param job_arguments: Arguments to be passed to EMR
        step. Input arguments is a dictionary with Key - Variable name to check Airflow variables, Value - default
        value of the EMR job if Airflow variable is not found. EMR step uses the KEY name passed in the dictionary
        and check whether an Airflow Variable created with the name and fetch the value from Airflow variable if that
        is the case. By default, EMR step appends airflow run id to get the Airflow Variable name if the multiple run
        parameter is True otherwise it will not append run id
        :param task_id: Name of the task id to be displayed in the Airflow DAG
        :return: EMR Task
        """
        if job_arguments is None:
            job_arguments = {}
        if task_id:
            task_id = '_' + task_id
        else:
            task_id = ''

        start_job_id = "emr_job" + task_id

        def create_studio(studio_name):

            client = boto3.client("emr")
            response = client.create_studio(
                Name=studio_name,
                Description=f"Studio for {studio_name}",
                AuthMode='IAM',
                VpcId=AWS_VPC,
                SubnetIds=SUBNETS,
                ServiceRole=EMR_STUDIO_SERVICE_ROLE,
                WorkspaceSecurityGroupId=EMR_STUDIO_WORKSPACE_SECURITY_GROUP_ID,
                EngineSecurityGroupId=EMR_STUDIO_ENGINE_SECURITY_GROUP_ID,
                DefaultS3Location=EMR_STUDIO_DEFAULT_S3_LOCATION,
            )

        def create_app(application_name, whitelist="false", **context):


            network_config = {
                'subnetIds': SUBNETS,
                'securityGroupIds': EMR_SERVERLESS_APPLICATION_SECURITY_GROUP
            }
            autostop_config = {
                'enabled': True,
                'idleTimeoutMinutes': 5
            }

            workerConfiguration = {
                'cpu': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['cpu'],
                'memory': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['memory'],
                'disk': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['disk']
            }

            initialCapacity = {
                'EXECUTOR': {
                    'workerCount': 1,
                    'workerConfiguration': workerConfiguration
                }
            }

            client = boto3.client("emr-serverless")
            response = client.list_applications(
                maxResults=50
            )['applications']

            applications_existed = []

            for r in response:
                applications_existed.append(r['name'])

            print(applications_existed)

            if application_name not in applications_existed:
                create_application = EmrServerlessCreateApplicationOperator(
                    task_id="create_emr_serverless_application",
                    release_label=EMR_VERSION,
                    job_type="SPARK",
                    config={"name": application_name,
                            "networkConfiguration": network_config,
                            "autoStopConfiguration": autostop_config,
                            "initialCapacity": initialCapacity
                            },
                )

                self.application_id = create_application.execute(context=context)
            else:
                raise AirflowException(
                    f"{application_name} is already created. Same application can't be created again")

        def delete_app(application_id, **context):
            print(f"deleting application id - {application_id}")
            delete_application_task = EmrServerlessDeleteApplicationOperator(
                task_id="delete_emr_application",
                application_id=application_id,
            )

            delete_application_task.execute(context=context)
            print(f"deleted application id - {application_id}")

        def submit_job(job_arguments, **context):

            modified_arguments = []
            if self.multiple_runs:
                for k, v in job_arguments.items():
                    value = Variables.get(k + "_" + context['dag_run'].run_id, v)
                    modified_arguments.append(value)
            else:
                for k, v in job_arguments.items():
                    value = Variables.get(k, v)
                    modified_arguments.append(value)

            modified_arguments = [str(arg) for arg in modified_arguments]

            print("Job is being submitted to : " + self.application_id)
            print("Job running with parameters : " + ','.join(modified_arguments))
            SPARK_JOB_DRIVER = {
                "sparkSubmit": {
                    "entryPoint": EMR_HISTORICAL_LOAD_SPARK_SCRIPT,
                    "entryPointArguments": modified_arguments,
                    "sparkSubmitParameters": f"--conf spark.submit.pyFiles={ETL_ZIP_PATH} "
                                             f"--conf spark.jars={EMR_JDBC_DEPENDENCY_JARS} "
                                             f"--conf spark.archives={EMR_PYSPARK_VENV} "
                                             "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                                             "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                                             "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                                             f"--conf spark.files={ETL_CONF_PATH} "
                                             "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory "
                                             f" {EMR_SERVERLESS_JOB_INITIALISATION_OPTIONS}",
                }
            }
            submit_job_init = EmrServerlessStartJobOperator(
                task_id='start_job',
                application_id=self.application_id,
                execution_role_arn=EMR_JOB_ROLE_ARN,
                job_driver=SPARK_JOB_DRIVER,
                configuration_overrides=DEFAULT_MONITORING_CONFIG,
                config={'name': job_name},
                waiter_max_attempts=500,
                waiter_delay=60
            )

            submit_job_init.execute(context=context)

        submit_job_task = PythonOperator(
            task_id=start_job_id,
            provide_context=True,
            python_callable=submit_job,
            op_kwargs={'job_arguments': job_arguments},
            trigger_rule=TriggerRule.ONE_SUCCESS,
            on_success_callback=notify_task_success_to_slack,
            on_failure_callback=notify_task_failure_to_slack
        )

        if self.create_app:
            return create_app(application_name=self.application_name)
        if self.delete_app:
            return delete_app(application_id=self.application_id)
        if self.create_studio:
            return create_studio(studio_name=self.studio_name)
        else:
            return submit_job_task


######## Class with EMR deferrable ########

class EMR_WITH_DEFERRABLE:

    def __init__(self, application_id, application_name=None, multiple_runs=False, create_app=False, delete_app=False,
                create_studio=False, studio_name=None):
        if create_app:
            self.application_id = '0000'
        else:
            self.application_id = application_id
        self.multiple_runs = multiple_runs
        self.application_name = application_name
        self.create_app = create_app
        self.delete_app = delete_app
        self.white_list = white_list
        self.create_studio = create_studio
        self.studio_name = studio_name

    def run(
            self,
            job_name: str = None,
            job_arguments=None,
            task_id=None
    ):
        """
        :param job_name: Name of the job to be displayed in EMR
        :param job_arguments: Arguments to be passed to EMR
        step. Input arguments is a dictionary with Key - Variable name to check Airflow variables, Value - default
        value of the EMR job if Airflow variable is not found. EMR step uses the KEY name passed in the dictionary
        and check whether an Airflow Variable created with the name and fetch the value from Airflow variable if that
        is the case. By default, EMR step appends airflow run id to get the Airflow Variable name if the multiple run
        parameter is True otherwise it will not append run id
        :param task_id: Name of the task id to be displayed in the Airflow DAG
        :return: EMR Task
        """
        if job_arguments is None:
            job_arguments = {}
        if task_id:
            task_id = '_' + task_id
        else:
            task_id = ''

        start_job_id = "emr_job" + task_id

        def create_studio(studio_name):

            client = boto3.client("emr")
            response = client.create_studio(
                Name=studio_name,
                Description=f"Studio for {studio_name}",
                AuthMode='IAM',
                VpcId=AWS_VPC,
                SubnetIds=SUBNETS,
                ServiceRole=EMR_STUDIO_SERVICE_ROLE,
                WorkspaceSecurityGroupId=EMR_STUDIO_WORKSPACE_SECURITY_GROUP_ID,
                EngineSecurityGroupId=EMR_STUDIO_ENGINE_SECURITY_GROUP_ID,
                DefaultS3Location=EMR_STUDIO_DEFAULT_S3_LOCATION,
            )

        def create_app(application_name, whitelist="false", **context):

            network_config = {
                'subnetIds': SUBNETS,
                'securityGroupIds': EMR_SERVERLESS_APPLICATION_SECURITY_GROUP
            }
            autostop_config = {
                'enabled': True,
                'idleTimeoutMinutes': 5
            }

            workerConfiguration = {
                'cpu': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['cpu'],
                'memory': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['memory'],
                'disk': EMR_SERVERLESS_APPLICATION_INITIAL_CAPACITY['disk']
            }

            initialCapacity = {
                'EXECUTOR': {
                    'workerCount': 1,
                    'workerConfiguration': workerConfiguration
                }
            }

            client = boto3.client("emr-serverless")
            response = client.list_applications(
                maxResults=50
            )['applications']

            applications_existed = []

            for r in response:
                applications_existed.append(r['name'])

            print(applications_existed)

            if application_name not in applications_existed:
                create_application = EmrServerlessCreateApplicationOperator(
                    task_id="create_emr_serverless_application",
                    release_label=EMR_VERSION,
                    job_type="SPARK",
                    config={"name": application_name,
                            "networkConfiguration": network_config,
                            "autoStopConfiguration": autostop_config,
                            "initialCapacity": initialCapacity
                            },
                )

                self.application_id = create_application.execute(context=context)
            else:
                raise AirflowException(
                    f"{application_name} is already created. Same application can't be created again")

        def delete_app(application_id, **context):
            print(f"deleting application id - {application_id}")
            delete_application_task = EmrServerlessDeleteApplicationOperator(
                task_id="delete_emr_application",
                application_id=application_id,
            )

            delete_application_task.execute(context=context)
            print(f"deleted application id - {application_id}")

        def job_config(job_arguments, **context):

            unique_key = None
            if job_arguments.get('unique_key', None):
                unique_key = job_arguments.get('unique_key', None)

            if unique_key:
                unique_id = unique_key + "_" + context['dag_run'].run_id
            else:
                unique_id = context['dag_run'].run_id

            modified_arguments = []
            if self.multiple_runs:
                for k, v in job_arguments.items():
                    value = Variables.get(k + "_" + unique_id, v)
                    modified_arguments.append(value)
            else:
                for k, v in job_arguments.items():
                    value = Variables.get(k, v)
                    modified_arguments.append(value)

            modified_arguments = [str(arg) for arg in modified_arguments]

            print("Job is being submitted to : " + self.application_id)
            print("Job running with parameters : " + ','.join(modified_arguments))
            
            DEFAULT_MONITORING_CONFIG = {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"{EMR_LOGS_URI}"}
                },
            SPARK_JOB_DRIVER = {
                "job_driver": {
                    "sparkSubmit": {
                        "entryPoint": EMR_HISTORICAL_LOAD_SPARK_SCRIPT,
                        "entryPointArguments": modified_arguments,
                        "sparkSubmitParameters": f"--conf spark.submit.pyFiles={ETL_ZIP_PATH} "
                                                 f"--conf spark.jars={EMR_JDBC_DEPENDENCY_JARS} "
                                                 f"--conf spark.archives={EMR_PYSPARK_VENV} "
                                                 "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "
                                                 "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "
                                                 "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                                                 f"--conf spark.files={ETL_CONF_PATH} "
                                                 "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory "
                                                 f" {EMR_SERVERLESS_JOB_INITIALISATION_OPTIONS}",
                    }
                }
            }

            context['task_instance'].xcom_push(key=f'unique_id', value=unique_id)
            Variables.set(unique_id, SPARK_JOB_DRIVER)


        fetch_params_task = PythonOperator(
            task_id="fetch_params_" + start_job_id,
            provide_context=True,
            python_callable=job_config,
            op_kwargs={'job_arguments': job_arguments},
            trigger_rule=TriggerRule.ONE_SUCCESS,
            on_success_callback=notify_task_success_to_slack,
            on_failure_callback=notify_task_failure_to_slack
        )

        emr_job_task = CustomEmrServerlessJobOperator(
            task_id=start_job_id,
            application_id=self.application_id,
            job_name = job_name,
            execution_role_arn=EMR_JOB_ROLE_ARN,
            on_success_callback=notify_task_success_to_slack,
            on_failure_callback=notify_task_failure_to_slack
        )

        return fetch_params_task, emr_job_task


class CustomEmrServerlessJobSensor(BaseTrigger):
    def __init__(self, application_id, job_run_id, aws_region):
        super().__init__()
        self.application_id = application_id
        self.job_run_id = job_run_id
        self.aws_region = aws_region

    def serialize(self):
        return ("folder.generic_folder.CustomEmrServerlessJobSensor",
                {"application_id": self.application_id, "job_run_id": self.job_run_id, "aws_region": self.aws_region})

    async def run(self):
        client = boto3.client("emr-serverless", region_name=self.aws_region)
        while True:
            response = client.get_job_run(
                applicationId=self.application_id,
                jobRunId=self.job_run_id
            )
            status = response["jobRun"]["state"]

            if status in ["SUCCESS", "FAILED", "CANCELLED"]:
                yield TriggerEvent({"status": status, "job_run_id": self.job_run_id})
                return

            await asyncio.sleep(60)


class CustomEmrServerlessJobOperator(BaseOperator):
    def __init__(self, application_id, job_name, execution_role_arn, aws_conn_id="aws_default",
                 region_name="us-east-1", **kwargs):
        super().__init__(**kwargs)
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.job_name = job_name

    def execute(self, context: Context):
        unique_id = context['task_instance'].xcom_pull(key=f'unique_id')
        print("unique id is")
        print(unique_id)
        job_params = Variables.get(unique_id)
        print("job parameters are ")
        print(job_params)

        hook = EmrServerlessHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        response = hook.conn.start_job_run(
            applicationId=self.application_id,
            name = self.job_name,
            executionRoleArn=self.execution_role_arn,
            jobDriver=job_params["job_driver"],
            configurationOverrides=job_params.get("configuration_overrides", {})
        )

        job_run_id = response["jobRunId"]
        self.log.info(f"Started EMR job: {job_run_id}")

        self.defer(trigger=CustomEmrServerlessJobSensor(self.application_id, job_run_id, self.region_name),
                   method_name="job_waiter")

    def job_waiter(self, context: Context, event=None):
        status = event["status"]
        if status == "SUCCESS":
            self.log.info(f"EMR job {event['job_run_id']} completed successfully.")
            return
        else:
            raise Exception(f"EMR job {event['job_run_id']} failed or was cancelled.")
