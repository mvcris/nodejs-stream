import {Transform, PassThrough, pipeline, TransformCallback} from 'stream';
import {createGzip} from 'zlib';
import {Storage} from '@google-cloud/storage';

const fetchData = async () => {
  const response = await fetch(
    'https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv'
  );
  return response.body;
};

const storage = new Storage({keyFilename: 'credentials.json'});
const gzip = createGzip();
const passThrough = new PassThrough();
const writeStream = storage
  .bucket('my-bucket')
  .file('output.gzip')
  .createWriteStream();

fetchData().then(async res => {
  if (!res) {
    throw new Error('');
  }
  const decoder = new TextDecoder();
  const reader = res.getReader();
  let isDone = false;
  while (!isDone) {
    const {done, value} = await reader.read();
    isDone = done;
    if (value) {
      const decodedValue = decoder.decode(value, {stream: true});
      passThrough.push(decodedValue);
    }
  }
  passThrough.end();
});

class CustomTransform extends Transform {
  constructor() {
    super();
  }

  _transform(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    const data = chunk.toString().toLowerCase().replace(/\s+/g, ' ').trim();
    this.push(data);
    callback();
  }
}

pipeline(passThrough, new CustomTransform(), gzip, writeStream, error => {
  console.log(error);
});
