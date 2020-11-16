from wialon import Wialon, WialonError
import logging

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.INFO)


def get_engine_hours(
        reportResourceId=14587158,
        reportTemplateId=29,
        start_timestamp=1604174400,
        end_timestamp=1604519999,
        reportObjectId=15021052):
    try:
        logger.debug('Creating wialon instance')
        wialon_api = Wialon()
        # old username and password login is deprecated, use token login
        result = wialon_api.token_login(
            token='8ad9141cc16c305ffa5f0fff9df4e273BFE4CB6398F53061F6315C002FAD111C8271BD93')
        wialon_api.sid = result['eid']

        result = wialon_api.avl_evts()

        logger.debug('Eval events %s', result)
        # exit()
        result = wialon_api.report_cleanup_result(
            {}
        )
        logger.debug('report_cleanup_result %s', result)
        result = wialon_api.report_exec_report(
            {
                "reportResourceId": reportResourceId,
                "reportTemplateId": reportTemplateId,
                "reportTemplate": None,
                "reportObjectId": reportObjectId,
                "reportObjectSecId": 0,
                "interval": {"flags": 117440512, "from": start_timestamp, "to": end_timestamp},
                "remoteExec": 1,
                "reportObjectIdList": []
            }
        )

        # print("report_exec_report ", result)  # {"remoteExec":1}
        logger.debug('report_exec_report %s', result)

        all_units_rows = []

        if result.get('remoteExec') == 1:
            """ Wait until result response comes back to you """
            while True:
                result = wialon_api.report_get_report_status(
                    {}
                )
                # print("report_get_report_status", result)
                logger.debug('report_get_report_status %s', result)
                if result.get('status') == "4":
                    result = wialon_api.report_apply_report_result(
                        {}
                    )
                    logger.debug('report_apply_report_result %s', result)
                    # print("report_apply_report_result", result)

                    break
            result = wialon_api.report_select_result_rows(
                {"tableIndex": 0,
                 "config": {"type": "range", "data": {"from": 0, "to": 0xfffff, "level": 0, "unitInfo": 1}}}
            )
            # print("Select Top Level: ", result)
            logger.debug('report_select_result_rows %s', result)

            index = 0
            batch_rows = []
            for r in result:
                # print("Top Level Rows: ", r)
                batch_rows.append(
                    {
                        "svc": "report/select_result_rows",
                        "params": {
                            "tableIndex": 0,
                            "config": {"type": "row", "data": {"rows": [f"{index}"], "level": 0, "unitInfo": 1}}
                        }
                    }
                )
                index += 1

            # print("core/batch", batch_rows)
            unit_rows = wialon_api.core_batch(batch_rows)
            # logger.debug('core_batch %s', len(unit_rows))
            for unit_data in unit_rows:
                all_units_rows += unit_data
            logger.debug('All Rows:  %s', len(all_units_rows))
            # print("All Rows: ", len(all_units_rows))
        wialon_api.core_logout()

        return all_units_rows
    except WialonError as e:
        logger.error('Wialon Error %s', e)
        return []
