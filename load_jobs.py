from src.db.crud.jobs import create_job_with_config
from src.db.crud.job_schedules import read_job_schedule
from src.db.crud.process_types import select_process_type
from src.db.models import ConnectionData
from src.db.crud import read_record


class LoadJobs:
    """
    Class designed to handle the creation of jobs using business and image data obtained 
    from various types of data sources
    """
    def __init__(self):
        """
        Initializes LoadJobs with pre-defined job mappings 
        and default schedule name.
        """
        self.default_schedule_name = "Hourly"
        self.jobs_mapping = {
            "biz_to_cde": {"source": "biz_connection", "target": "CDE-BIZ_CONN"},
            "image_to_cde": {"source": "image_connection", "target": "CDE-IMAGE_CONN"},
            "image_to_he": {"source": "image_connection", "target": "HE-TARGET_CONN"},
            "he_to_cde": {"source": "HE-TAP_CONN", "target": "CDE-HE_CONN"},
            "dis_to_cde": {
                "source": "DIS-COLLAGE_CONN",
                "target": "CDE-COLLAGE_CONN",
            },
        }

    async def get_default_job_schedule_pk(self):
        """
        Retrieves and returns the primary key of the predefined default job schedule.
        
        Returns:
            job_schedule_pk (str): primary key of the default job schedule
        """
        job_schedule = await read_job_schedule(
            self.default_schedule_name, "schedule_name"
        )

        return job_schedule.job_schedule_pk

    async def get_connection_pk(self, connection_name):
        """
        Retrieves and returns the primary key of a connection based on its name.
        
        Args: 
            connection_name (str): name of the specific connection.
        
        Returns:
            connection_pk (str): primary key of the connection.
        """
        connection_record = await read_record(
            connection_name, "connection", "connection_name"
        )
        print(connection_record)
        connection_obj = ConnectionData(**connection_record)

        return connection_obj.connection_pk

    async def create_jobs_from_data(self, jobs_data):
        """
        Creates jobs using the provided data.
        
        Args:
            jobs_data (list): list of dictionaries containing job data.
        """
        for job_data in jobs_data:
            await create_job_with_config(**job_data)

    async def dynamically_populate_job_data(self, job_name, data):
        """
        Populates necessary job data dynamically based on job name and the
        data provided.
        
        Args:
            job_name (str): name of the job.
            data (dict): data to be utilised for job data population.
        
        Returns:
            (dict): dynamically populated job data.
        """
        unique_id = (
            data["connection_pk"] if data.get("connection_pk") else data["collage_pk"]
        )
        return {
            "job_name": f"{job_name}_{unique_id}",
            "connection_source_fk": data["connection_pk"]
            if data.get("connection_pk")
            else await self.get_connection_pk(self.jobs_mapping[job_name]["source"]),
            "connection_target_fk": await self.get_connection_pk(
                self.jobs_mapping[job_name]["target"]
            ),
            "job_schedule_fk": await self.get_default_job_schedule_pk(),
        }

    async def create_job_post_collage(self, collage):
        """
        Creates a job after completion of a collage task.
        
        Args:
            collage (obj): collage object.
        """
        job_data = await self.dynamically_populate_job_data(
            "dis_to_cde", collage.__dict__
        )
        job_data["job_config"] = {
            "TARGET_NAMING_CONVENTION": "raw-collage-{partition_key}",
            "TIMESTAMP_KEY": "-{timestamp}.csv",
            "TARGET_PARTITION_KEY": "id",
            "TARGET_CUSTOM_METADATA": {},
        }

        await self.create_jobs_from_data([job_data])

    async def create_job_post_bizz_connection(self, connections_data):
         """
        Creates a job after a business data connection is established.
        
        Args:
            connections_data (obj): business connection data.
        """
        job_data = await self.dynamically_populate_job_data(
            "biz_to_cde", connections_data.__dict__
        )

        job_data["job_config"] = {
            "TARGET_NAMING_CONVENTION": f"raw-biz-{connections_data.connection_pk}",
            "TIMESTAMP_KEY": "-{timestamp}.csv",
            "TARGET_CUSTOM_METADATA": {
                "CONNECTION_NAME": f"{connections_data.connection_name}",
                "JOB_CREATOR": "admin",
            },
        }

        await self.create_jobs_from_data([job_data])

    async def create_job_post_image_connection(self, connections_data, user_guid):
         """
        Creates a job after an image data connection is established.
        
        Args:
            connections_data (obj): image connection data.
            user_guid (str): Guid of the user for whom the job is to be created.
        """
        image_to_cde_job = await self.dynamically_populate_job_data(
            "image_to_cde", connections_data.__dict__
        )
        image_to_cde_job["job_config"] = {
            "TARGET_NAMING_CONVENTION": f"raw-image-{connections_data.connection_pk}",
            "TIMESTAMP_KEY": "-{timestamp}.csv",
            "TARGET_CUSTOM_METADATA": {
                "CONNECTION_NAME": f"{connections_data.connection_name}",
                "JOB_CREATOR": "admin",
            },
        }
        image_to_he_job = await self.dynamically_populate_job_data(
            "image_to_he", connections_data.__dict__
        )
        image_to_he_job["job_config"] = {
            "TARGET_NAMING_CONVENTION": f"raw-he-{connections_data.connection_pk}",
            "CUSTOM_COLUMN_VALUE": str(connections_data.connection_pk),
            "EXECUTION_USER_GUID": str(user_guid),
        }

        he_to_cde_job = await self.dynamically_populate_job_data(
            "he_to_cde", connections_data.__dict__
        )

        he_to_cde_job["job_config"] = {
            "CONNECTION_PK": str(connections_data.connection_pk),
            "TARGET_NAMING_CONVENTION": f"raw-he-{connections_data.connection_pk}",
            "TIMESTAMP_KEY": "-{timestamp}.csv",
            "TARGET_CUSTOM_METADATA": {
                "CONNECTION_NAME": f"{connections_data.connection_name}",
                "JOB_CREATOR": str(user_guid),
            },
        }

        he_to_cde_job["connection_source_fk"] = await self.get_connection_pk(
            self.jobs_mapping["he_to_cde"]["source"]
        )

        await self.create_jobs_from_data(
            [image_to_cde_job, image_to_he_job, he_to_cde_job]
        )

    async def create_job_post_connection(self, connections_data, user_guid):
        """
        Creates a job after a specified type of connection (business or image) is established. 
        
        Args: 
            connections_data (obj): connection data.
            user_guid (str): Guid of the user for whom the job is to be created.
        """
        process_type = await select_process_type(connections_data.process_type_fk)
        process_type_name = process_type.process_type_name

        if process_type_name == "Business Source":
            await self.create_job_post_bizz_connection(connections_data)
        elif process_type_name == "Image Source":
            await self.create_job_post_image_connection(connections_data, user_guid)
