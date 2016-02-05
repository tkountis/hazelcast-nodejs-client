/* tslint:disable */
import ClientMessage = require('../ClientMessage');
import ImmutableLazyDataList = require('./ImmutableLazyDataList');
import {BitsUtil} from '../BitsUtil';
import Address = require('../Address');
import {Data} from '../serialization/Data';
import {MapMessageType} from './MapMessageType';

var REQUEST_TYPE = MapMessageType.MAP_KEYSETWITHPAGINGPREDICATE;
var RESPONSE_TYPE = 106;
var RETRYABLE = false;


export class MapKeySetWithPagingPredicateCodec {


    static calculateSize(name:string, predicate:Data) {
        // Calculates the request payload size
        var dataSize:number = 0;
        dataSize += BitsUtil.calculateSizeString(name);
        dataSize += BitsUtil.calculateSizeData(predicate);
        return dataSize;
    }

    static encodeRequest(name:string, predicate:Data) {
        // Encode request into clientMessage
        var clientMessage = ClientMessage.newClientMessage(this.calculateSize(name, predicate));
        clientMessage.setMessageType(REQUEST_TYPE);
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.appendString(name);
        clientMessage.appendData(predicate);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    static decodeResponse(clientMessage:ClientMessage, toObjectFunction:(data:Data) => any = null) {
        // Decode response from client message
        var parameters:any = {};
        var responseSize = clientMessage.readInt32();
        var response:any = [];
        for (var responseIndex = 0; responseIndex <= responseSize; responseIndex++) {
            var responseItem = toObjectFunction(clientMessage.readData());

            response.push(responseItem)
        }
        parameters['response'] = new ImmutableLazyDataList(response, toObjectFunction);
        return parameters;

    }


}
