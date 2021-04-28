import { IncomingMessage, ServerResponse } from 'http';
import anylogger from 'anylogger';
import config from '@mmstudio/config';
import { Client, ClientOptions } from 'minio';
import range_parser from 'range-parser';

const client = new Client(config.minio as ClientOptions);
const logger = anylogger('@mmstudio/an000046');
const NAME_SPACE = 'mmstudio';

interface IMetaData {
	originialfilename: string;
	'content-type': string;
}

export default async function an46(
	id: string,
	download: string | 'true' | 'false' | boolean,
	req: IncomingMessage,
	res: ServerResponse
) {
	if (!id) {
		logger.debug('method: getfile,id is empty');
		throw new Error('id can not be empty!');
	}
	logger.debug('Request headers:', JSON.stringify(req.headers));
	const none_match = req.headers['if-none-match'];
	logger.debug(`method: getfile,file_id:${id}`);
	try {
		const stat = await client.statObject(NAME_SPACE, id);
		// Etag标识
		const etag = `W/"${stat.etag}"`;
		res.setHeader('Etag', etag);
		// 增加Etag判断文件是否有变动
		if (none_match && none_match === etag) {
			// 文件没有变动直接返回304使用本地缓存
			res.removeHeader('Content-Type');
			res.removeHeader('Content-Length');
			res.removeHeader('Transfer-Encoding');
			res.statusCode = 304;
			res.end();
			return;
		}
		res.statusCode = 200;

		const meta = stat.metaData as IMetaData;
		res.setHeader('Content-Type', meta['content-type']);
		const filename = meta.originialfilename;
		if (download !== undefined && download !== false && download !== 'false') {
			// 强制下载文件
			logger.debug(`method: getfile,download: true,file_name:${filename}`);
			if (download === true || download === 'true') {
				res.setHeader('Content-Disposition', `attachment; filename=${filename}`);
			} else {
				// rename, `download` should be a filename
				res.setHeader('Content-Disposition', `attachment; filename=${download}`);
			}
		} else {
			res.setHeader('Content-Disposition', `inline; filename=${filename}`);
		}
		const r = req.headers['range'];
		if (r) {
			logger.info(`method: getfile,id:${id} with range:${r}`);
			const ranges = range_parser(stat.size, r, { combine: true });
			logger.debug(`parsed range:${JSON.stringify(ranges)}`);
			if (ranges === -1) {
				res.setHeader('Content-Range', `*/${stat.size}`);
				throw new Error('Incorrect request!');
			} else if (ranges === -2) {
				throw new Error('Incorrect request!');
			} else {
				const range = ranges[0];
				const start = range.start;
				const end = range.end; // for lastest byte
				res.statusCode = 206;
				res.setHeader('Content-Range', `bytes ${start}-${end}/${stat.size}`);
				res.setHeader('Content-Length', end + 1 - start);
				const stream = await client.getPartialObject(
					NAME_SPACE,
					id,
					start,
					end - start + 1
				);
				stream.pipe(res);
			}
		} else {
			logger.debug(`method: getfile,id:${id} without range.`);
			const stream = await client.getObject(NAME_SPACE, id);
			stream.pipe(res);
		}
	} catch (e) {
		const er = e as Error;
		const err = er.message || er.toString();
		logger.error('read file fail!', err);
		res.statusCode = 304;
		res.end(err);
	}
}
