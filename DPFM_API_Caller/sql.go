package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-production-order-conf-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-production-order-conf-cancels-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE header.ProductionOrder = %d ", input.header.ProductionOrder),
		fmt.Sprintf("AND header.ProductionOrderItem = %d ", input.header.ProductionOrderItem),
		fmt.Sprintf("AND header.Operations = %d ", input.header.Operations),
		fmt.Sprintf("AND header.OperationsItem = %d ", input.header.OperationsItem),
		fmt.Sprintf("AND header.OperationID = %d ", input.header.OperationID),
		fmt.Sprintf("AND header.ConfirmationCountingID = %d ", input.header.ConfirmationCountingID),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	header.ProductionOrder,
    	header.ProductionOrderItem,
    	header.Operations,
    	header.OperationsItem,
    	header.OperationID,
    	header.ConfirmationCountingID,
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_production_order_confirmation_header_data as header 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

