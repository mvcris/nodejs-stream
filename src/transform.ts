import {createReadStream, createWriteStream} from 'fs';
import {Transform, TransformCallback} from 'stream';

const readStream = createReadStream('./crimes_chicago.csv');
const writeStream = createWriteStream('./crimes_chicago.json');

let currentChunkData = '';
let isFirstLine = true;
let isFirstElement = true;

const getJsonArrayDelimiter = () => {
  if (isFirstElement) {
    isFirstElement = false;
    return '[';
  }
  return ',';
};

const checkAndTransformLine = (line: string) => {
  const data = line.split(',');
  const year = data[17];
  if (year <= '2008') {
    return;
  }
  return {
    caseNumber: data[1],
    type: data[5],
    description: data[6],
    year,
  };
};

const transform = new Transform({
  readableObjectMode: true,
  writableObjectMode: true,
  transform(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback
  ) {
    currentChunkData += chunk.toString();
    const lines = currentChunkData.split('\n');
    currentChunkData = lines.pop() || '';
    for (const line of lines) {
      if (isFirstLine) {
        isFirstLine = false;
        continue;
      }
      this.push(getJsonArrayDelimiter());
      const data = checkAndTransformLine(line);
      if (data) this.push(JSON.stringify(data));
    }
    callback();
  },
  flush(callback: TransformCallback) {
    if (currentChunkData) {
      this.push(getJsonArrayDelimiter());
      const data = checkAndTransformLine(currentChunkData);
      if (data) this.push(JSON.stringify(data));
    }
    this.push(']');
    callback();
  },
});

readStream.pipe(transform).pipe(writeStream);
