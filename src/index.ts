import { IncomingMessage, ServerResponse } from 'http';
import { readFile, rm, stat, writeFile } from 'fs/promises';
import { createReadStream } from 'fs';
import anylogger from 'anylogger';
import { Client, ClientOptions } from 'minio';
import range_parser from 'range-parser';
import an61 from '@mmstudio/an000061';

const logger = anylogger('@mmstudio/an000046');
let gClient: Client;

interface IMetaData {
	originialfilename: string;
	'content-type': string;
}

export default async function an46(
	id: string,
	download: string | 'true' | 'false' | boolean,
	req: IncomingMessage,
	res: ServerResponse,
	encrypt: boolean
) {
	if (!id) {
		logger.debug('method: getfile,id is empty');
		throw new Error('id can not be empty!');
	}
	logger.debug('Request headers:', JSON.stringify(req.headers));
	const none_match = req.headers['if-none-match'];
	logger.debug(`method: getfile,file_id:${id}`);
	const namespace = getNameSpace();
	const client = getClient();
	const itemStat = await client.statObject(namespace, id);
	// Etag标识
	const etag = `W/"${itemStat.etag}"`;
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

	const meta = itemStat.metaData as IMetaData;
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
		const ranges = range_parser(itemStat.size, r, { combine: true });
		logger.debug(`parsed range:${JSON.stringify(ranges)}`);
		if (ranges === -1) {
			if (encrypt) {
				const tmp = `/tmp/${id}`;
				await client.fGetObject(namespace, id, tmp);
				const buf = await readFile(tmp);
				an61.decrypt(buf);
				await writeFile(tmp, an61.decrypt(buf));
				const s = await stat(tmp);
				await rm(tmp);
				res.setHeader('Content-Range', `*/${s.size}`);
			} else {
				res.setHeader('Content-Range', `*/${itemStat.size}`);
			}
			throw new Error('Incorrect request!');
		} else if (ranges === -2) {
			throw new Error('Incorrect request!');
		} else {
			if (encrypt) {
				throw new Error('Incorrect request!');
			}
			const range = ranges[0];
			const start = range.start;
			const end = range.end; // for lastest byte
			res.statusCode = 206;
			res.setHeader('Content-Range', `bytes ${start}-${end}/${itemStat.size}`);
			res.setHeader('Content-Length', end + 1 - start);
			const stream = await client.getPartialObject(
				namespace,
				id,
				start,
				end - start + 1
			);
			stream.pipe(res);
		}
	} else {
		logger.debug(`method: getfile,id:${id} without range.`);
		if (encrypt) {
			const tmp = `/tmp/${id}`;
			await client.fGetObject(namespace, id, tmp);
			const buf = await readFile(tmp);
			an61.decrypt(buf);
			await writeFile(tmp, an61.decrypt(buf));
			const fs = createReadStream(tmp);
			fs.on('close', async () => {
				await rm(tmp);
			});
			fs.pipe(res);
		} else {
			const stream = await client.getObject(namespace, id);
			stream.pipe(res);
		}
	}
}

function getClient() {
	if (!gClient) {
		gClient = new Client(getConfig());
	}
	return gClient;
}

function getNameSpace() {
	return process.env.MINIO_NAME_SPACE || 'mmstudio';
}

let gConfig: ClientOptions;
function getConfig() {
	if (!gConfig) {
		gConfig = JSON.parse(process.env.MINIO_CONFIG!) as ClientOptions;
	}
	return gConfig;
}
