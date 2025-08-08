module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    roots: ['<rootDir>/src'],
    testMatch: [
        '**/__tests__/**/*.test.ts',
        '**/__tests__/**/*.spec.ts'
    ],
    transform: {
        '^.+\\.ts$': ['ts-jest', {
            tsconfig: {
                // TypeScript config for tests
                target: 'es2020',
                module: 'commonjs',
                lib: ['es2020'],
                allowJs: true,
                strict: true,
                esModuleInterop: true,
                skipLibCheck: true,
                forceConsistentCasingInFileNames: true,
                resolveJsonModule: true,
                moduleResolution: 'node',
                types: ['node', 'jest']
            }
        }]
    },
    collectCoverageFrom: [
        'src/**/*.ts',
        '!src/**/*.d.ts',
        '!src/**/__tests__/**'
    ],
    coverageDirectory: 'coverage',
    coverageReporters: ['text', 'lcov', 'html'],
    moduleFileExtensions: ['ts', 'js', 'json'],
    testTimeout: 10000
}